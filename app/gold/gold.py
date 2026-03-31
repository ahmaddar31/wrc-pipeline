"""Gold stage — data mart layer.

Reads processed documents for a date range, enriches them with extracted
plain text and analytics-ready fields, and upserts into the `gold_decisions`
MongoDB collection.

Enrichments added at this stage:
  - plain_text: stripped HTML with no tags, ready for full-text search / NLP
  - word_count: number of whitespace-separated tokens in the plain text
  - has_pdf: whether the original source file was a PDF
  - gold_processed_at: ISO timestamp of when this record entered the gold layer
"""

import argparse
import os
import re

from bs4 import BeautifulSoup

from app.common.config import get_config
from app.common.logging_utils import get_json_logger, log_json
from app.storage.minio_client import ObjectStorageClient
from app.storage.mongo_client import MongoMetadataClient


def _extract_plain_text(html: str) -> str:
    """Return whitespace-normalised plain text from an HTML string."""
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(separator=" ")
    return re.sub(r"\s+", " ", text).strip()


def main(start_date: str, end_date: str) -> None:
    config = get_config()
    logger = get_json_logger("wrc_gold", config.log_dir, "gold.jsonl")
    storage = ObjectStorageClient()
    mongo = MongoMetadataClient()

    documents = mongo.fetch_processed_by_date_range(start_date, end_date)

    log_json(
        logger, "info", "gold_stage_started",
        start_date=start_date,
        end_date=end_date,
        total_documents=len(documents),
    )

    success_count = 0
    failed_count = 0

    for doc in documents:
        identifier = doc.get("identifier", "<unknown>")
        try:
            file_type = doc.get("file_type", "")
            plain_text = ""
            word_count = 0

            if file_type == "html":
                object_path = doc["object_storage_path"]
                bucket, object_name = object_path.split("/", 1)
                local_path = os.path.join(config.local_tmp_dir, f"gold_{identifier}.html")
                storage.download_file(bucket, object_name, local_path)
                html = open(local_path, "r", encoding="utf-8").read()
                plain_text = _extract_plain_text(html)
                word_count = len(plain_text.split())

            gold_doc = {
                # --- identity ---
                "source": doc.get("source"),
                "identifier": identifier,
                # --- provenance ---
                "body": doc.get("body"),
                "body_id": doc.get("body_id"),
                "partition_date": doc.get("partition_date"),
                # --- decision metadata ---
                "title": doc.get("title"),
                "description": doc.get("description"),
                "published_date": doc.get("published_date"),
                "published_date_iso": doc.get("published_date_iso"),
                "detail_url": doc.get("detail_url"),
                # --- file info ---
                "file_type": file_type,
                "has_pdf": file_type == "pdf",
                "file_hash": doc.get("file_hash"),
                "processed_object_path": doc.get("object_storage_path"),
                # --- enrichments ---
                "plain_text": plain_text,
                "word_count": word_count,
            }

            mongo.upsert_gold_decision(gold_doc)
            success_count += 1

            log_json(
                logger, "info", "gold_record_upserted",
                identifier=identifier,
                word_count=word_count,
                file_type=file_type,
            )

        except Exception as exc:
            failed_count += 1
            log_json(
                logger, "error", "gold_record_failed",
                identifier=identifier,
                error=str(exc),
            )

    log_json(
        logger, "info", "gold_stage_finished",
        start_date=start_date,
        end_date=end_date,
        success=success_count,
        failed=failed_count,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    args = parser.parse_args()
    main(args.start_date, args.end_date)
