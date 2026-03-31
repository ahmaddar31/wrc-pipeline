import os
from pathlib import Path

from scrapy.exceptions import DropItem

from app.common.config import get_config
from app.common.hashing import sha256_file
from app.common.logging_utils import get_json_logger, log_json
from app.storage.minio_client import ObjectStorageClient
from app.storage.mongo_client import MongoMetadataClient

# Fields that are pipeline-internal and should not be persisted to MongoDB
_TRANSIENT_FIELDS = {"html_content", "downloaded_file_path"}


class DecisionFilePipeline:
    """Single pipeline that:
    1. Writes the file (HTML or binary) to a local temp path.
    2. Computes SHA-256 of the file.
    3. Skips upload+upsert if an identical hash already exists in MongoDB
       (idempotency — re-running the same date range is a no-op for unchanged
       files).
    4. Uploads to MinIO landing bucket.
    5. Upserts metadata into MongoDB landing collection.
    6. Emits a structured JSON log entry for every record processed.
    """

    def __init__(self) -> None:
        self.config = get_config()
        self.storage = ObjectStorageClient()
        self.mongo = MongoMetadataClient()
        self.logger = get_json_logger("wrc_pipeline", self.config.log_dir, "ingestion.jsonl")
        os.makedirs(self.config.local_tmp_dir, exist_ok=True)

    @classmethod
    def from_crawler(cls, crawler):
        return cls()

    def _drop(self, identifier, reason: str, **extra):
        log_json(self.logger, "error", "item_dropped", identifier=identifier, reason=reason, **extra)
        raise DropItem(f"{reason} | identifier={identifier}")

    def process_item(self, item, spider):
        identifier = item.get("identifier")
        if not identifier:
            log_json(self.logger, "error", "item_dropped", identifier=None, reason="missing_identifier")
            raise DropItem("Missing identifier")

        file_type = item.get("file_type")
        if not file_type:
            self._drop(identifier, "missing_file_type")

        extension = file_type.lower()
        # Object name inside the bucket: partition/body_id/identifier.ext
        object_name = (
            f"{item['partition_date']}/{item['body_id']}/{identifier}.{extension}"
        )
        local_path = os.path.join(self.config.local_tmp_dir, f"{identifier}.{extension}")

        # --- Write file locally ---
        if extension == "html":
            html_content = item.get("html_content")
            if not html_content:
                self._drop(identifier, "missing_html_content")
            Path(local_path).write_text(html_content, encoding="utf-8")
        else:
            downloaded_path = item.get("downloaded_file_path")
            if not downloaded_path or not os.path.exists(downloaded_path):
                self._drop(identifier, "binary_file_missing", downloaded_path=downloaded_path)
            local_path = downloaded_path

        file_hash = sha256_file(local_path)

        # --- Idempotency check: skip if hash unchanged ---
        existing = self.mongo.get_landing_by_identifier(item["source"], identifier)
        if existing and existing.get("file_hash") == file_hash:
            log_json(
                self.logger,
                "info",
                "unchanged_file_skipped",
                identifier=identifier,
                partition_date=item["partition_date"],
                body=item["body"],
                existing_path=existing.get("object_storage_path"),
            )
            item["file_hash"] = file_hash
            item["object_storage_path"] = existing.get("object_storage_path")
            item["local_tmp_path"] = local_path
            self._upsert_mongo(item)
            return item

        # --- Upload to MinIO ---
        content_type = "text/html; charset=utf-8" if extension == "html" else None
        object_storage_path = self.storage.upload_file(
            bucket_name=self.config.minio_landing_bucket,
            object_name=object_name,
            file_path=local_path,
            content_type=content_type,
        )

        item["local_tmp_path"] = local_path
        item["object_storage_path"] = object_storage_path
        item["file_hash"] = file_hash

        self._upsert_mongo(item)

        log_json(
            self.logger,
            "info",
            "landing_record_upserted",
            identifier=identifier,
            partition_date=item["partition_date"],
            body=item["body"],
            published_date=item.get("published_date"),
            object_storage_path=object_storage_path,
            file_hash=file_hash,
            file_type=extension,
        )

        return item

    def _upsert_mongo(self, item) -> None:
        """Strip transient pipeline fields before persisting to MongoDB."""
        doc = {k: v for k, v in dict(item).items() if k not in _TRANSIENT_FIELDS}
        self.mongo.upsert_landing_metadata(doc)
