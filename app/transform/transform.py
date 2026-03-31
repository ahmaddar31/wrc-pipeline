import argparse
import os
from bs4 import BeautifulSoup

from app.common.config import get_config
from app.common.hashing import sha256_file
from app.common.logging_utils import get_json_logger, log_json
from app.storage.minio_client import ObjectStorageClient
from app.storage.mongo_client import MongoMetadataClient


def extract_relevant_html(html_text: str) -> str:
    """Extract the main decision content from a WRC detail page.

    The WRC site wraps the actual decision in:
        <div class="col-sm-9">
            <h1 class="page-title">...</h1>
            <div class="content">...</div>   ← the decision body
        </div>

    We target that `div.content` specifically.  If it is not found (e.g. the
    site changes layout), we fall back to stripping known chrome elements and
    returning whatever is left in <body>.
    """
    soup = BeautifulSoup(html_text, "lxml")

    # --- Primary strategy: grab the decision content div directly ---
    content_div = soup.select_one("div.col-sm-9 div.content")
    if content_div:
        # Also include the page title that sits just above it
        title_tag = soup.select_one("h1.page-title")
        wrapper = soup.new_tag("div")
        if title_tag:
            wrapper.append(title_tag.__copy__())
        wrapper.append(content_div.__copy__())
        return str(wrapper)

    # --- Fallback: strip chrome and return body ---
    for selector in [
        "header", "footer", "nav", "script", "style",
        "#globalCookieBar", ".top-header", ".logo-header",
        ".searchbanner", "#binderFixed", ".return-to-search",
        ".breadcrumb", ".cookie", ".menu",
    ]:
        for tag in soup.select(selector):
            tag.decompose()

    return str(soup.body or soup)


def main(start_date: str, end_date: str) -> None:
    config = get_config()
    logger = get_json_logger("wrc_transform", config.log_dir, "transform.jsonl")
    storage = ObjectStorageClient()
    mongo = MongoMetadataClient()

    os.makedirs(config.local_processed_dir, exist_ok=True)

    documents = mongo.fetch_landing_by_date_range(start_date, end_date)

    log_json(
        logger,
        "info",
        "transform_started",
        start_date=start_date,
        end_date=end_date,
        total_documents=len(documents),
    )

    for doc in documents:
        try:
            source = doc["source"]
            identifier = doc["identifier"]
            object_path = doc["object_storage_path"]
            file_type = doc["file_type"]

            bucket, object_name = object_path.split("/", 1)
            local_input_path = os.path.join(config.local_tmp_dir, os.path.basename(object_name))
            storage.download_file(bucket, object_name, local_input_path)

            new_extension = "html" if file_type == "html" else file_type
            output_filename = f"{identifier}.{new_extension}"
            local_output_path = os.path.join(config.local_processed_dir, output_filename)

            if file_type == "html":
                html_text = open(local_input_path, "r", encoding="utf-8").read()
                cleaned_html = extract_relevant_html(html_text)
                with open(local_output_path, "w", encoding="utf-8") as f:
                    f.write(cleaned_html)
            else:
                with open(local_input_path, "rb") as src, open(local_output_path, "wb") as dst:
                    dst.write(src.read())

            new_hash = sha256_file(local_output_path)
            processed_object_name = f"processed/{doc['partition_date']}/{output_filename}"
            processed_path = storage.upload_file(
                bucket_name=config.minio_processed_bucket,
                object_name=processed_object_name,
                file_path=local_output_path,
                content_type="text/html" if new_extension == "html" else None,
            )

            processed_doc = {
                "source": source,
                "identifier": identifier,
                "body": doc.get("body"),
                "body_id": doc.get("body_id"),
                "title": doc.get("title"),
                "description": doc.get("description"),
                "published_date": doc.get("published_date"),
                "published_date_iso": doc.get("published_date_iso"),
                "partition_date": doc.get("partition_date"),
                "detail_url": doc.get("detail_url"),
                "file_type": new_extension,
                "file_hash": new_hash,
                "object_storage_path": processed_path,
                "input_object_storage_path": doc.get("object_storage_path"),
            }

            mongo.upsert_processed_metadata(processed_doc)

            log_json(
                logger,
                "info",
                "transform_record_processed",
                identifier=identifier,
                source_object=doc.get("object_storage_path"),
                processed_object=processed_path,
                file_type=new_extension,
                file_hash=new_hash,
            )

        except Exception as exc:
            log_json(
                logger,
                "error",
                "transform_record_failed",
                identifier=doc.get("identifier"),
                error=str(exc),
            )

    log_json(
        logger,
        "info",
        "transform_finished",
        start_date=start_date,
        end_date=end_date,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    args = parser.parse_args()

    main(args.start_date, args.end_date)