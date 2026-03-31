"""Scrapy spider for workplacerelations.ie Decisions and Determinations.

Accepts start_date and end_date (YYYY-MM-DD) as spider arguments and iterates
over monthly partitions × all four bodies, following pagination on each search
results page and the detail page for each record.

HTML structure confirmed against the live site (2024):

  Search result card  →  <li class="each-item clearfix">
    Identifier        →  <span class="refNO">LCR22912</span>
    Description       →  first <div class="col-sm-9"> text (parties)
    Date              →  <span class="date">30/01/2024</span>
    Detail link       →  <a class="btn btn-primary" href="/en/cases/...">

  Pagination          →  <a class="next" href="?...&pageNumber=N">

  Detail page
    Main content      →  <div class="content"> inside <div class="col-sm-9">
    Doc download      →  <a href="...(.pdf|.doc|.docx)"> (case-specific only)
"""

from __future__ import annotations

import os
import re
from datetime import datetime
from urllib.parse import urlencode

import scrapy

from app.common.config import get_config
from app.common.dates import week_ranges, parse_cli_date, to_site_date
from app.common.logging_utils import get_json_logger, log_json
from app.scraper.items import DecisionItem


# Four adjudicating bodies available via the left-hand filter on the search page
BODIES: dict[str, str] = {
    "1": "Employment Appeals Tribunal",
    "2": "Equality Tribunal",
    "3": "Labour Court",
    "15376": "Workplace Relations Commission",
}

BASE_URL = "https://www.workplacerelations.ie"
SEARCH_PATH = "/en/search/"

# Generic site-level document keywords — we never treat these as case documents
_GENERIC_DOC_TOKENS = ("cookie", "guide", "policy", "publication", "form", "accessibility")


class WorkplaceRelationsSpider(scrapy.Spider):
    name = "workplace_relations"
    allowed_domains = ["workplacerelations.ie"]

    def __init__(self, start_date: str | None = None, end_date: str | None = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if not start_date or not end_date:
            raise ValueError("start_date and end_date are required (YYYY-MM-DD)")

        self.config = get_config()
        self.start_date = parse_cli_date(start_date)
        self.end_date = parse_cli_date(end_date)
        self._logger = get_json_logger("wrc_spider", self.config.log_dir, "spider.jsonl")

        # Per-(partition, body) stats — emitted as JSON on spider close
        self._stats: dict[str, dict] = {}

    # ------------------------------------------------------------------
    # Entry points
    # ------------------------------------------------------------------

    def start_requests(self):
        for part_start, part_end in week_ranges(self.start_date, self.end_date):
            partition_date = part_start.strftime("%Y-W%W")

            for body_id, body_name in BODIES.items():
                params = {
                    "decisions": "1",
                    "from": to_site_date(part_start),
                    "to": to_site_date(part_end),
                    "body": body_id,
                }
                url = f"{BASE_URL}{SEARCH_PATH}?{urlencode(params)}"
                key = f"{partition_date}|{body_id}"
                self._stats[key] = {
                    "partition_date": partition_date,
                    "body_id": body_id,
                    "body_name": body_name,
                    "found": 0,
                    "success": 0,
                    "failed": 0,
                    "failed_urls": [],
                }

                log_json(
                    self._logger, "info", "partition_started",
                    partition_date=partition_date,
                    body_id=body_id,
                    body_name=body_name,
                    search_url=url,
                )

                yield scrapy.Request(
                    url=url,
                    callback=self.parse_search,
                    errback=self._handle_error,
                    meta={
                        "body_id": body_id,
                        "body_name": body_name,
                        "partition_date": partition_date,
                        "search_url": url,
                        "key": key,
                    },
                )

    # ------------------------------------------------------------------
    # Search results page
    # ------------------------------------------------------------------

    def parse_search(self, response):
        meta = response.meta
        key = meta["key"]
        cards = response.css("li.each-item")
        self._stats[key]["found"] += len(cards)

        for card in cards:
            # Identifier: reference number in the bottom row of the card
            identifier = self._clean(card.css("span.refNO::text").get())

            # Description: parties / case text in the top row (first col-sm-9)
            raw_desc = " ".join(
                t.strip() for t in card.css("div.row:first-child div.col-sm-9 *::text").getall()
                if t.strip()
            )
            description = self._clean(raw_desc)

            # Published date
            published_date = self._clean(card.css("span.date::text").get())
            published_date_iso = self._to_iso_date(published_date)

            # Link to detail page
            detail_href = card.css("a.btn-primary::attr(href)").get()

            if not identifier:
                self._stats[key]["failed"] += 1
                log_json(
                    self._logger, "warning", "identifier_missing",
                    card_html=card.get()[:400],
                    search_url=meta["search_url"],
                )
                continue

            item = DecisionItem(
                source="workplace_relations",
                body=meta["body_name"],
                body_id=meta["body_id"],
                identifier=identifier,
                title=identifier,           # On this site the identifier IS the title
                description=description,
                published_date=published_date,
                published_date_iso=published_date_iso,
                partition_date=meta["partition_date"],
                search_url=meta["search_url"],
                detail_url=response.urljoin(detail_href) if detail_href else None,
                file_url=None,
                file_type=None,
                local_tmp_path=None,
                object_storage_path=None,
                file_hash=None,
            )

            if item["detail_url"]:
                yield response.follow(
                    item["detail_url"],
                    callback=self.parse_detail,
                    errback=self._handle_error,
                    meta={**meta, "item": item},
                )
            else:
                self._stats[key]["failed"] += 1
                log_json(
                    self._logger, "error", "detail_url_missing",
                    identifier=identifier,
                    partition_date=meta["partition_date"],
                    body_name=meta["body_name"],
                )

        # Pagination: <a class="next"> is the reliable next-page indicator
        next_href = response.css("a.next::attr(href)").get()
        if next_href:
            yield response.follow(
                next_href,
                callback=self.parse_search,
                errback=self._handle_error,
                meta=meta,
            )

    # ------------------------------------------------------------------
    # Detail page
    # ------------------------------------------------------------------

    def parse_detail(self, response):
        item = response.meta["item"]
        key = response.meta["key"]

        try:
            # Enrich description from the fully server-rendered detail page.
            # The search results page renders card text via JS so Scrapy can't
            # read it — the detail page is always static HTML.
            detail_description = self._extract_description(response)
            if detail_description:
                item["description"] = detail_description

            doc_href = self._find_document_link(response, item["identifier"])

            if doc_href:
                file_url = response.urljoin(doc_href)
                extension = file_url.rstrip("/").rsplit(".", 1)[-1].split("?")[0].lower()
                tmp_path = os.path.join(
                    self.config.local_tmp_dir,
                    f"{item['identifier']}.{extension}",
                )
                yield scrapy.Request(
                    url=file_url,
                    callback=self.parse_binary_file,
                    errback=self._handle_error,
                    meta={**response.meta, "tmp_path": tmp_path, "file_type": extension},
                )
            else:
                # No downloadable file — store the decision HTML page itself
                item["file_url"] = response.url
                item["file_type"] = "html"
                item["html_content"] = response.text
                self._stats[key]["success"] += 1
                yield item

        except Exception as exc:
            self._stats[key]["failed"] += 1
            self._stats[key]["failed_urls"].append(response.url)
            log_json(
                self._logger, "error", "detail_processing_failed",
                identifier=item["identifier"],
                detail_url=item["detail_url"],
                error=str(exc),
            )

    # ------------------------------------------------------------------
    # Binary file download (PDF / DOC / DOCX)
    # ------------------------------------------------------------------

    def parse_binary_file(self, response):
        item = response.meta["item"]
        tmp_path = response.meta["tmp_path"]
        file_type = response.meta["file_type"]
        key = response.meta["key"]

        try:
            os.makedirs(os.path.dirname(os.path.abspath(tmp_path)), exist_ok=True)
            with open(tmp_path, "wb") as fh:
                fh.write(response.body)

            item["file_url"] = response.url
            item["file_type"] = file_type
            item["downloaded_file_path"] = tmp_path
            self._stats[key]["success"] += 1
            yield item

        except Exception as exc:
            self._stats[key]["failed"] += 1
            self._stats[key]["failed_urls"].append(response.url)
            log_json(
                self._logger, "error", "binary_file_save_failed",
                identifier=item["identifier"],
                file_url=response.url,
                http_status=response.status,
                error=str(exc),
            )

    # ------------------------------------------------------------------
    # Errback
    # ------------------------------------------------------------------

    def _handle_error(self, failure):
        request = failure.request
        key = request.meta.get("key", "unknown")
        item = request.meta.get("item")

        if key in self._stats:
            self._stats[key]["failed"] += 1
            self._stats[key]["failed_urls"].append(request.url)

        log_json(
            self._logger, "error", "request_failed",
            url=request.url,
            identifier=item["identifier"] if item else None,
            error=str(failure.value),
        )

    # ------------------------------------------------------------------
    # Spider close — emit per-partition summary
    # ------------------------------------------------------------------

    def closed(self, reason: str):
        for stats in self._stats.values():
            log_json(
                self._logger, "info", "partition_summary",
                partition_date=stats["partition_date"],
                body_id=stats["body_id"],
                body_name=stats["body_name"],
                records_found=stats["found"],
                records_success=stats["success"],
                records_failed=stats["failed"],
                failed_urls=stats["failed_urls"],
                close_reason=reason,
            )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _find_document_link(response, identifier: str) -> str | None:
        """Return href of a PDF/DOC/DOCX link that belongs to this specific case.

        Excludes generic site documents (cookie policy, guides, forms) by
        checking for known generic keywords in the URL.
        """
        for href in response.css("a[href]::attr(href)").getall():
            lower = href.lower()
            if not any(lower.endswith(ext) for ext in (".pdf", ".doc", ".docx")):
                continue
            if any(token in lower for token in _GENERIC_DOC_TOKENS):
                continue
            # Accept if the URL contains the case identifier or the /cases/ path
            if identifier.lower() in lower or "/cases/" in lower:
                return href
        return None

    @staticmethod
    def _clean(value: str | None) -> str | None:
        if not value:
            return None
        cleaned = re.sub(r"\s+", " ", value).strip()
        return cleaned or None

    @staticmethod
    def _extract_description(response) -> str | None:
        """Extract a human-readable case description from the detail page.

        The detail page content is fully server-rendered. We look for a
        "PARTIES:" label in the decision text, which reliably contains the
        appellant vs respondent names (e.g. "SONOMA VALLEY AND A WORKER").
        Falls back to the first meaningful line of the content div.
        """
        content = response.css("div.col-sm-9 div.content")
        if not content:
            return None

        full_text = content.css("*::text").getall()
        full_text = [t.strip() for t in full_text if t.strip()]

        # Look for "PARTIES:" label and grab the text that follows
        for i, token in enumerate(full_text):
            if "PARTIES" in token.upper():
                # Collect the next 1–3 tokens as the parties description
                parts = [t for t in full_text[i + 1: i + 4] if t and t not in (":", "-")]
                if parts:
                    return " ".join(parts)[:200]

        # Fallback: first non-trivial line of content
        for token in full_text:
            if len(token) > 15 and not token.isupper():
                return token[:200]

        return None

    @staticmethod
    def _to_iso_date(value: str | None) -> str | None:
        if not value:
            return None
        for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(value.strip(), fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None
