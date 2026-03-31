# WRC Scraping Pipeline

A production-grade data pipeline that scrapes legal decisions from
[workplacerelations.ie](https://www.workplacerelations.ie/en/cases/),
stores raw files in MinIO, metadata in MongoDB, and processes data through
Bronze → Silver → Gold layers orchestrated by Apache Airflow.

---

## Table of Contents

1. [Stack](#stack)
2. [Architecture](#architecture)
3. [Data Flow](#data-flow)
4. [Project Structure](#project-structure)
5. [Quick Start](#quick-start)
6. [Configuration](#configuration)
7. [Airflow Connections](#airflow-connections)
8. [Running the Pipeline](#running-the-pipeline)
9. [Pipeline Stages](#pipeline-stages)
10. [Data Model](#data-model)
11. [Error Handling & Logging](#error-handling--logging)
12. [Idempotency](#idempotency)
13. [Code Walkthrough](#code-walkthrough)

---

## Stack

| Component | Technology |
|-----------|------------|
| Scraper | Scrapy 2.11 |
| Metadata store | MongoDB 7 |
| File / object store | MinIO |
| Orchestration | Apache Airflow 2.9.2 (Celery executor) |
| Task queue | Redis |
| Airflow DB | PostgreSQL 16 |
| Infrastructure | Docker Compose |

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                     Apache Airflow                       │
│  scrape → transform → quality_check → gold → aggregate  │
└────────────────────────┬────────────────────────────────┘
                         │ orchestrates
         ┌───────────────┼───────────────┐
         ▼               ▼               ▼
    ┌─────────┐    ┌──────────┐    ┌──────────┐
    │ Scrapy  │    │  MinIO   │    │ MongoDB  │
    │ Spider  │───▶│ (files)  │    │(metadata)│
    └─────────┘    └──────────┘    └──────────┘
                   wrc-landing      landing_metadata
                   wrc-processed    processed_metadata
                                    gold_decisions
                                    monthly_stats
```

---

## Data Flow

Data moves through four MongoDB collections and two MinIO buckets across five pipeline stages:

```
WRC Website
    │
    ▼ scrape_landing_zone
    ├── MinIO:   wrc-landing/YYYY-MM/body_id/identifier.{html,pdf,doc}
    └── MongoDB: landing_metadata
    │
    ▼ transform_processed_zone
    ├── MinIO:   wrc-processed/processed/YYYY-MM/identifier.{html,pdf,doc}
    └── MongoDB: processed_metadata
    │
    ▼ quality_check
    │   validates processed_metadata — fails pipeline if >20% records are broken
    │
    ▼ gold_stage
    └── MongoDB: gold_decisions  (+ plain_text, word_count, has_pdf)
    │
    ▼ aggregate_stats
    └── MongoDB: monthly_stats  (decisions per body per month)
```

---

## Project Structure

```
.
├── dags/
│   └── wrc_pipeline_dag.py          # Airflow DAG — 5-task pipeline
│
├── app/
│   ├── common/
│   │   ├── config.py                # All configuration via env vars
│   │   ├── logging_utils.py         # JSON structured logging
│   │   ├── dates.py                 # Date utilities
│   │   └── hashing.py               # SHA-256 file hashing
│   │
│   ├── scraper/
│   │   ├── settings.py              # Scrapy settings (throttle, retry, etc.)
│   │   ├── items.py                 # DecisionItem schema
│   │   ├── pipelines.py             # Upload to MinIO + upsert to MongoDB
│   │   ├── middlewares.py           # Rotating User-Agent middleware
│   │   └── spiders/
│   │       └── workplace_relations.py  # Main spider
│   │
│   ├── storage/
│   │   ├── mongo_client.py          # All MongoDB operations
│   │   └── minio_client.py          # All MinIO operations
│   │
│   ├── transform/
│   │   └── transform.py             # HTML cleaning (Silver layer)
│   │
│   ├── quality/
│   │   └── check.py                 # Data quality validation
│   │
│   └── gold/
│       ├── gold.py                  # Gold enrichment (plain text, word count)
│       └── aggregate.py             # Monthly statistics data mart
│
├── docker-compose.yml
├── requirements.txt
└── .env
```

---

## Quick Start

### Prerequisites

- Docker + Docker Compose v2

### 1. Configure environment

```bash
cp .env.example .env
# Edit .env — set AIRFLOW_ALERT_EMAIL and SMTP_* for email alerts
```

### 2. Start all infrastructure

```bash
docker-compose up -d
```

### 3. Initialise Airflow (first time only)

```bash
docker-compose run --rm airflow-init
```

This will:
- Install Python dependencies
- Create the Airflow database schema
- Create the admin user (`admin` / `admin`)
- Register the `mongo_wrc` and `minio_wrc` Airflow Connections

### 4. Open the services

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow UI | http://localhost:8080 | admin / admin |
| MinIO console | http://localhost:9001 | minioadmin / minioadmin |
| MongoDB | localhost:27017 | root / root |

---

## Configuration

All settings are read from environment variables in `.env`. No values are hardcoded in source code.

| Variable | Default | Description |
|----------|---------|-------------|
| `MONGO_URI` | `mongodb://root:root@mongo:27017/` | MongoDB connection string |
| `MONGO_DB` | `wrc_pipeline` | Database name |
| `MONGO_LANDING_COLLECTION` | `landing_metadata` | Raw scrape metadata |
| `MONGO_PROCESSED_COLLECTION` | `processed_metadata` | Cleaned file metadata |
| `MONGO_GOLD_COLLECTION` | `gold_decisions` | Enriched decisions |
| `MONGO_STATS_COLLECTION` | `monthly_stats` | Aggregated stats data mart |
| `MINIO_ENDPOINT` | `minio:9000` | MinIO host:port |
| `MINIO_LANDING_BUCKET` | `wrc-landing` | Raw files bucket |
| `MINIO_PROCESSED_BUCKET` | `wrc-processed` | Cleaned files bucket |
| `SCRAPER_DOWNLOAD_DELAY` | `1.0` | Base delay between requests (seconds) |
| `SCRAPER_CONCURRENT_REQUESTS` | `8` | Max parallel Scrapy requests |
| `RETRY_TIMES` | `3` | HTTP retry attempts on 429/5xx |
| `AIRFLOW_ALERT_EMAIL` | _(empty)_ | Email to receive failure alerts |
| `SMTP_HOST` | `smtp.gmail.com` | SMTP server for email alerts |
| `SMTP_USER` | _(empty)_ | SMTP username |
| `SMTP_PASSWORD` | _(empty)_ | SMTP password / App Password |

---

## Airflow Connections

Credentials are stored in Airflow Connections (Admin → Connections in the UI) rather than plain environment variables. The DAG reads them at runtime and injects them as environment variables into each task.

| Connection ID | Type | Purpose |
|---------------|------|---------|
| `mongo_wrc` | Generic | MongoDB host, port, username, password |
| `minio_wrc` | Generic | MinIO host, port, access key, secret key |

If a connection is not registered, the pipeline falls back to the values in `.env` automatically.

To update credentials after deployment: edit the connection in the Airflow UI — no restart needed.

---

## Running the Pipeline

### Option A — Airflow UI (recommended)

1. Open http://localhost:8080
2. Enable the DAG `wrc_ingestion_transformation_pipeline`
3. Click **Trigger DAG w/ config** and provide:

```json
{
  "start_date": "2024-01-01",
  "end_date": "2024-03-31"
}
```

If no config is provided, the pipeline defaults to the previous 30 days.

### Option B — Command line (per stage)

```bash
# Scrape
python -m scrapy crawl workplace_relations \
  -a start_date=2024-01-01 \
  -a end_date=2024-03-31

# Transform
python -m app.transform.transform \
  --start-date 2024-01-01 --end-date 2024-03-31

# Quality check
python -m app.quality.check \
  --start-date 2024-01-01 --end-date 2024-03-31

# Gold stage
python -m app.gold.gold \
  --start-date 2024-01-01 --end-date 2024-03-31

# Aggregate stats
python -m app.gold.aggregate \
  --start-date 2024-01-01 --end-date 2024-03-31
```

---

## Pipeline Stages

### 1. `scrape_landing_zone` — Bronze Layer

**What it does:** Crawls workplacerelations.ie for all decisions published in the date range, downloads the files, and stores them in the landing zone.

**How it works:**
- Iterates over every month in the date range × 4 adjudication bodies
- For each combination, paginates through search results
- Follows each result to its detail page
- Downloads PDF/DOC files if available; saves the HTML page itself otherwise
- Writes the file to MinIO (`wrc-landing`)
- Writes metadata to MongoDB (`landing_metadata`)

**Failure behaviour:** Per-record errors (parse failure, download failure) are caught, logged with the record identifier and reason, and skipped. The task only fails if a critical error occurs (e.g. cannot connect to MinIO or MongoDB).

**Timeout:** 2 hours

---

### 2. `transform_processed_zone` — Silver Layer

**What it does:** Reads every landing record for the date range, cleans the HTML content, and writes the result to the processed zone.

**How it works:**
- Fetches landing metadata from MongoDB
- Downloads each file from MinIO `wrc-landing`
- For HTML files: extracts the decision content div, strips navigation/headers/footers
- For PDF/DOC files: passes through unchanged
- Uploads cleaned file to MinIO (`wrc-processed`)
- Upserts metadata to MongoDB (`processed_metadata`)

**Failure behaviour:** Per-record errors are caught and logged. The task continues processing remaining records.

**Timeout:** 1 hour

---

### 3. `quality_check` — Validation Gate

**What it does:** Validates the quality of processed data before promotion to the gold layer. Fails the pipeline if too many records are broken.

**Checks performed:**

| Check | Description |
|-------|-------------|
| Required fields | Every landing record must have: `identifier`, `source`, `title`, `published_date_iso`, `body`, `file_type`, `object_storage_path`, `file_hash` |
| Processing coverage | Every landing record must have a corresponding processed record |
| Failure rate | If more than 20% of records fail either check, the task exits with code 1 |

**Zero records** is treated as valid — no decisions published in a month is a normal business scenario.

**Retries:** 1 retry with 2-minute delay (fewer than other tasks — repeated quality failures indicate a real data problem, not a transient error).

**Timeout:** 15 minutes

---

### 4. `gold_stage` — Gold Layer

**What it does:** Enriches processed records with analytics-ready fields and writes them to the gold collection.

**Enrichments added:**

| Field | Description |
|-------|-------------|
| `plain_text` | Full decision text with all HTML tags stripped — ready for search or NLP |
| `word_count` | Number of words in `plain_text` — proxy for decision complexity |
| `has_pdf` | Boolean flag — whether the original file was a PDF |
| `gold_processed_at` | Timestamp when this record entered the gold layer |

**Timeout:** 1 hour

---

### 5. `aggregate_stats` — Data Mart

**What it does:** Runs a MongoDB aggregation pipeline over `gold_decisions` and writes one summary document per (body × month) into `monthly_stats`.

**Output document example:**

```json
{
  "body": "Workplace Relations Commission",
  "body_id": "15376",
  "year_month": "2024-03",
  "total_decisions": 42,
  "pdf_count": 30,
  "html_count": 12,
  "avg_word_count": 1850.5,
  "computed_at": "2024-04-01T00:00:00Z"
}
```

**Timeout:** 15 minutes

---

## Data Model

### `landing_metadata` — Raw scrape results

```json
{
  "source": "workplace_relations",
  "body": "Workplace Relations Commission",
  "body_id": "15376",
  "identifier": "ADJ-00012345",
  "title": "ADJ-00012345",
  "description": "Worker v Some Employer Ltd",
  "published_date": "15/03/2024",
  "published_date_iso": "2024-03-15",
  "partition_date": "2024-03",
  "detail_url": "https://www.workplacerelations.ie/en/cases/...",
  "file_url": "https://...",
  "file_type": "html",
  "object_storage_path": "wrc-landing/2024-03/15376/ADJ-00012345.html",
  "file_hash": "a1b2c3d4...",
  "created_at": "2024-04-01T00:00:00Z",
  "last_seen_at": "2024-04-01T00:00:00Z"
}
```

### `processed_metadata` — Cleaned files

Same structure as `landing_metadata` with:
- `object_storage_path` pointing to `wrc-processed`
- `input_object_storage_path` pointing to the original landing file
- Updated `file_hash` (hash of the cleaned content)

### `gold_decisions` — Enriched analytics layer

Adds to processed metadata:
- `plain_text` — full extracted text
- `word_count` — integer word count
- `has_pdf` — boolean
- `gold_processed_at` — timestamp

### `monthly_stats` — Data mart

```json
{
  "body": "Labour Court",
  "body_id": "3",
  "year_month": "2024-03",
  "total_decisions": 18,
  "pdf_count": 12,
  "html_count": 6,
  "avg_word_count": 2100.3,
  "computed_at": "2024-04-01T00:00:00Z"
}
```

### MongoDB Indexes

| Collection | Index | Type |
|-----------|-------|------|
| `landing_metadata` | `(source, identifier)` | Unique |
| `processed_metadata` | `(source, identifier)` | Unique |
| `gold_decisions` | `(source, identifier)` | Unique |
| `gold_decisions` | `published_date_iso` | Single field |
| `gold_decisions` | `body_id` | Single field |
| `monthly_stats` | `(body_id, year_month)` | Unique |

---

## Error Handling & Logging

### Per-record failure tracking

Every record that fails at any stage is individually logged with its identifier and the reason for failure. The pipeline never stops processing remaining records due to a single record failure.

**Failure events logged by the spider:**

| Event | Level | When |
|-------|-------|------|
| `identifier_missing` | WARNING | Card on search results has no reference number |
| `detail_url_missing` | ERROR | Card has no link to the detail page |
| `detail_processing_failed` | ERROR | Exception while parsing detail page |
| `binary_file_save_failed` | ERROR | Exception while writing downloaded file to disk |
| `request_failed` | ERROR | Network error, timeout, or unrecoverable HTTP error |

**Failure events logged by the pipeline:**

| Event | Level | When |
|-------|-------|------|
| `item_dropped` | ERROR | Record missing required fields (`identifier`, `file_type`, etc.) |
| `unchanged_file_skipped` | INFO | File hash matches existing record — skipped (idempotency) |
| `landing_record_upserted` | INFO | Record successfully stored |

### Per-partition summary

When the spider finishes, it emits one `partition_summary` log entry per (month × body) combination:

```json
{
  "message": "partition_summary",
  "partition_date": "2024-03",
  "body_id": "15376",
  "body_name": "Workplace Relations Commission",
  "records_found": 42,
  "records_success": 40,
  "records_failed": 2,
  "failed_urls": ["https://...", "https://..."],
  "close_reason": "finished"
}
```

### Log files

Logs are partitioned by month: `logs/YYYY-MM/filename.jsonl`

| File | Contents |
|------|----------|
| `logs/YYYY-MM/spider.jsonl` | Spider events — per-record failures, partition summaries |
| `logs/YYYY-MM/ingestion.jsonl` | Pipeline events — uploads, skips, drops |
| `logs/YYYY-MM/transform.jsonl` | Transform events — per-record success/failure |
| `logs/YYYY-MM/quality.jsonl` | Quality check results and per-record issues |
| `logs/YYYY-MM/gold.jsonl` | Gold stage and aggregation events |

### Airflow task retries

| Setting | Value |
|---------|-------|
| Retries | 2 (3 total attempts) |
| Retry delay | 5 minutes |
| Backoff | Exponential (5 → 10 → 20 min, capped at 30 min) |
| Execution timeout | 3 hours (2 for scrape, 1 for transform/gold, 15 min for quality/stats) |

### Email alerts

On final task failure (after all retries exhausted), an email is sent to `AIRFLOW_ALERT_EMAIL` containing:
- DAG name and task name
- Run ID and attempt number
- Direct link to the Airflow task log

Requires `SMTP_HOST`, `SMTP_USER`, and `SMTP_PASSWORD` to be set in `.env`. For Gmail, use an [App Password](https://support.google.com/accounts/answer/185833).

---

## Idempotency

Re-running the pipeline on the same date range is fully safe:

| Layer | Mechanism |
|-------|-----------|
| MongoDB | `upsert` on `(source, identifier)` — no duplicate documents |
| MinIO upload | SHA-256 hash compared before upload — unchanged files are not re-uploaded |
| Airflow | `catchup=False` — missed scheduled runs are not backfilled |

---

## Code Walkthrough

### `app/common/config.py`

Single source of truth for all configuration. Every module calls `get_config()` instead of reading `os.getenv()` directly. The `AppConfig` dataclass is `frozen=True` — immutable after creation, so no module can accidentally change a config value at runtime. All values have sensible defaults pointing to `localhost` so the app works locally without Docker; inside Docker, `.env` overrides them with service names (`mongo`, `minio`).

### `app/common/logging_utils.py`

Replaces Python's default plain-text logs with structured JSON logs — every log line is a valid JSON object parseable by log aggregators. Logs are partitioned by month (`logs/YYYY-MM/`) consistent with the data lake pattern used for MinIO and MongoDB. The `get_json_logger()` guard (`if logger.handlers: return logger`) prevents duplicate handlers when a module is imported multiple times.

### `app/storage/mongo_client.py`

All MongoDB operations in one class. Uses `upsert` with `$setOnInsert` for `created_at` — the creation timestamp is set only on the first insert and never overwritten on subsequent updates. Indexes are created on startup via `_ensure_indexes()` which is idempotent — safe to call every time.

### `app/storage/minio_client.py`

All MinIO operations. Buckets are created on startup if they don't exist. `upload_file()` returns the storage path string (`bucket/object_name`) which is what gets saved in MongoDB as `object_storage_path`. `object_exists()` catches `S3Error` because MinIO has no dedicated "exists" API — checking is done by attempting a stat call.

### `app/scraper/settings.py`

Scrapy configuration. `AUTOTHROTTLE` dynamically adjusts request speed based on server response times — prevents overloading the target site. `RANDOMIZE_DOWNLOAD_DELAY` adds jitter (0.5×–1.5× of the base delay) so requests don't arrive at perfectly regular intervals, which looks less like a bot. The custom `RotateUserAgentMiddleware` replaces the built-in one (`None` disables it) to rotate real browser User-Agent strings on every request.

### `app/scraper/items.py`

The `DecisionItem` schema defines all fields a scraped record can have. `html_content` and `downloaded_file_path` are marked as transient — they carry data from the spider to the pipeline but are stripped before saving to MongoDB, since the actual content belongs in MinIO.

### `app/scraper/middlewares.py`

`RotateUserAgentMiddleware` runs `process_request()` on every outgoing HTTP request, setting a random User-Agent from the `fake_useragent` database. The UA database is initialised once at module load (not per request) to avoid redundant network calls. Falls back to a hardcoded Chrome UA string if the library fails.

### `app/scraper/spiders/workplace_relations.py`

The spider iterates over every (month × body) combination in the date range and yields one search URL per pair. Scrapy processes these asynchronously. Each search page is paginated — `parse_search()` follows the "Next" link recursively until none exists. For each result card, the spider follows the detail page link. On the detail page it looks for a PDF/DOC download link; if found, it downloads the binary file; if not, it stores the HTML page itself. All callbacks have an `errback` pointing to `_handle_error()` — network failures are caught, logged, and counted without crashing the spider. On close, one `partition_summary` log entry is emitted per (month × body).

### `app/scraper/pipelines.py`

Receives every yielded item, validates required fields, writes the file locally, computes its SHA-256 hash, checks if the hash already exists in MongoDB (idempotency), uploads to MinIO, and upserts metadata to MongoDB. `DropItem` is used for validation failures — it tells Scrapy to silently discard the item without crashing. The `_drop()` helper logs the failure with the identifier and reason before raising.

### `app/transform/transform.py`

Fetches landing metadata from MongoDB, downloads each file from MinIO, and cleans it. HTML decisions are processed with BeautifulSoup — the primary strategy targets the specific `div.content` inside `div.col-sm-9` that the WRC site uses for decision bodies; a fallback strips known chrome elements (nav, header, footer, cookie bars) if the primary selector fails. PDF/DOC files pass through unchanged. Cleaned files are uploaded to `wrc-processed` and metadata is upserted to `processed_metadata`.

### `app/quality/check.py`

A validation gate between the Silver and Gold layers. Checks that every landing record has required fields and a corresponding processed record. Fails with `sys.exit(1)` if the failure rate exceeds 20% — Airflow treats a non-zero exit code as task failure, triggering retries and the email alert. Zero records is treated as valid (no decisions in a month is a normal scenario, not an error).

### `app/gold/gold.py`

Promotes processed records to the gold layer by adding analytics-ready enrichments: `plain_text` (HTML stripped to plain text via BeautifulSoup `get_text()`), `word_count` (length of `plain_text.split()`), and `has_pdf` (boolean). These fields make the collection useful for full-text search, NLP pipelines, and analytics dashboards without needing to re-process files.

### `app/gold/aggregate.py`

Runs a 4-stage MongoDB aggregation pipeline: `$match` (filter by date), `$addFields` (extract YYYY-MM from the ISO date), `$group` (count and average per body × month), `$sort`. Results are upserted into `monthly_stats` — the final data mart layer, ready for reporting.

### `dags/wrc_pipeline_dag.py`

The Airflow DAG defines the 5-task pipeline and its operational behaviour. `_conn_env()` reads credentials from Airflow Connections at DAG parse time and injects them as `env=` into each BashOperator — the app code reads `os.getenv()` and gets Connection values transparently. Jinja templates (`{{ dag_run.conf.get(...) }}`) allow manual date override at trigger time while defaulting to the previous 30 days for scheduled runs. The `>>` operator chain (`scrape >> transform >> quality_check >> gold_stage >> aggregate_stats`) defines the dependency order — if any task fails after all retries, all downstream tasks are skipped automatically.
