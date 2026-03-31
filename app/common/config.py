import os
from dataclasses import dataclass
from dotenv import load_dotenv


load_dotenv()


def _as_bool(value: str, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y"}


@dataclass(frozen=True)
class AppConfig:
    app_env: str
    log_level: str
    log_dir: str

    mongo_uri: str
    mongo_db: str
    mongo_landing_collection: str
    mongo_processed_collection: str

    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    minio_secure: bool
    minio_landing_bucket: str
    minio_processed_bucket: str

    local_tmp_dir: str
    local_processed_dir: str

    scraper_download_delay: float
    scraper_concurrent_requests: int
    scraper_autothrottle_enabled: bool

    default_partition_size: str
    request_timeout_seconds: int
    retry_times: int


def get_config() -> AppConfig:
    return AppConfig(
        app_env=os.getenv("APP_ENV", "local"),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        log_dir=os.getenv("LOG_DIR", "logs"),

        mongo_uri=os.getenv("MONGO_URI", "mongodb://root:root@localhost:27017/"),
        mongo_db=os.getenv("MONGO_DB", "wrc_pipeline"),
        mongo_landing_collection=os.getenv("MONGO_LANDING_COLLECTION", "landing_metadata"),
        mongo_processed_collection=os.getenv("MONGO_PROCESSED_COLLECTION", "processed_metadata"),

        minio_endpoint=os.getenv("MINIO_ENDPOINT", "localhost:9000"),
        minio_access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        minio_secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        minio_secure=_as_bool(os.getenv("MINIO_SECURE"), False),
        minio_landing_bucket=os.getenv("MINIO_LANDING_BUCKET", "wrc-landing"),
        minio_processed_bucket=os.getenv("MINIO_PROCESSED_BUCKET", "wrc-processed"),

        local_tmp_dir=os.getenv("LOCAL_TMP_DIR", "data/tmp"),
        local_processed_dir=os.getenv("LOCAL_PROCESSED_DIR", "data/processed"),

        scraper_download_delay=float(os.getenv("SCRAPER_DOWNLOAD_DELAY", "1.0")),
        scraper_concurrent_requests=int(os.getenv("SCRAPER_CONCURRENT_REQUESTS", "8")),
        scraper_autothrottle_enabled=_as_bool(os.getenv("SCRAPER_AUTOTHROTTLE_ENABLED"), True),

        default_partition_size=os.getenv("DEFAULT_PARTITION_SIZE", "monthly"),
        request_timeout_seconds=int(os.getenv("REQUEST_TIMEOUT_SECONDS", "30")),
        retry_times=int(os.getenv("RETRY_TIMES", "3")),
    )