from __future__ import annotations

import os
from minio import Minio
from minio.error import S3Error

from app.common.config import get_config


class ObjectStorageClient:
    def __init__(self) -> None:
        config = get_config()
        self.config = config
        self.client = Minio(
            endpoint=config.minio_endpoint,
            access_key=config.minio_access_key,
            secret_key=config.minio_secret_key,
            secure=config.minio_secure,
        )
        self._ensure_bucket(config.minio_landing_bucket)
        self._ensure_bucket(config.minio_processed_bucket)

    def _ensure_bucket(self, bucket_name: str) -> None:
        found = self.client.bucket_exists(bucket_name)
        if not found:
            self.client.make_bucket(bucket_name)

    def upload_file(self, bucket_name: str, object_name: str, file_path: str, content_type: str | None = None) -> str:
        self.client.fput_object(bucket_name, object_name, file_path, content_type=content_type)
        return f"{bucket_name}/{object_name}"

    def download_file(self, bucket_name: str, object_name: str, target_path: str) -> str:
        os.makedirs(os.path.dirname(target_path), exist_ok=True)
        self.client.fget_object(bucket_name, object_name, target_path)
        return target_path

    def object_exists(self, bucket_name: str, object_name: str) -> bool:
        try:
            self.client.stat_object(bucket_name, object_name)
            return True
        except S3Error:
            return False