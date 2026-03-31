# Architecture Decision Record

## Date Partition Size

Weekly partitions (`YYYY-Www`) were chosen over monthly for two reasons driven directly by the scale requirement (500k–1M+ documents):

1. **Smaller retry blast radius** — at high volume, a failed monthly partition could mean re-scraping thousands of records. A weekly partition caps the worst-case retry at ~250 records, making failures cheap to recover from.
2. **Lower data freshness lag** — decisions published on day 1 of a month are available in the processed layer within 7 days instead of up to 30. At scale this matters when downstream consumers (search indexes, dashboards) need recent data.

Weekly partitions do produce more Airflow runs (52/year vs 12), but each run is smaller and faster — which is the right trade-off when designing for 1000x volume. The `partition_date` key (`YYYY-Www`) is stored on every MongoDB record and reflected in MinIO paths and log directories, so any single week can be re-processed in isolation without touching others.

---

## Retries and Rate Limiting

**Three layers of protection:**

| Layer | Mechanism | Config |
|-------|-----------|--------|
| Network retries | Scrapy `RetryMiddleware` auto-retries on HTTP 429/500/502/503/504 | `RETRY_TIMES=3` |
| Speed control | `AutoThrottle` measures server latency and adjusts delay dynamically, targeting 4 concurrent requests | `AUTOTHROTTLE_TARGET_CONCURRENCY=4.0`, max delay 10s |
| Jitter | `RANDOMIZE_DOWNLOAD_DELAY` adds ±50% randomness to the base 1s delay so requests don't arrive at perfectly regular intervals | `DOWNLOAD_DELAY=1.0` |

On top of these, each request is sent with a random browser User-Agent (`fake-useragent`) to avoid fingerprinting. Airflow adds a fourth layer: task-level retries with exponential backoff (5 → 10 → 20 min, capped at 30 min) for infrastructure-level failures such as MinIO or MongoDB being temporarily unavailable.

---

## Deduplication Strategy

Idempotency is enforced at two independent levels so that re-running the pipeline on the same date range is always a safe no-op:

1. **SHA-256 hash check before upload** — before uploading a file to MinIO, the pipeline hashes the local file and compares it to the `file_hash` stored in MongoDB. If they match, the upload is skipped entirely. This keeps the landing zone immutable and avoids wasting bandwidth.

2. **MongoDB upsert on `(source, identifier)`** — every write uses `update_one(..., upsert=True)` with a unique index on `(source, identifier)`. Re-running never creates duplicate documents; it only updates the `last_seen_at` timestamp.

The combination means: unchanged content → no re-upload, metadata timestamp refreshed; changed content → new upload, hash updated, full audit trail preserved in structured JSON logs.

---

## Scaling to 50+ Sources

The current design separates concerns cleanly (spider → pipeline → transform), so adding sources is additive rather than invasive. The main changes needed:

- **Spider registry** — each source becomes its own Scrapy spider class in `app/scraper/spiders/`. The DAG accepts a `source` parameter and dispatches to the correct spider. A shared base class handles common logic (pagination, error handling, stats tracking); subclasses override only the site-specific CSS selectors and URL construction.

- **Config-driven source profiles** — base URL, body IDs, CSS selectors, and rate-limit settings move from hardcoded constants into a `sources` config table in MongoDB or a YAML file. Spider `__init__` loads its profile by name. Adding a new source requires no code change — only a new config entry.

- **Airflow dynamic task mapping** — replace the single `scrape` task with a dynamically mapped task that fans out one instance per `(source, partition_date)` pair. Airflow runs these concurrently within worker limits, reducing total pipeline runtime from O(sources × time) to O(time).

- **Isolated storage per source** — bucket and collection names follow `{source}-landing` / `{source}-processed` conventions. Each source is self-contained; a failure in one source does not affect others, and a source can be replayed independently.

- **Per-source rate-limit profiles** — `DOWNLOAD_DELAY`, `CONCURRENT_REQUESTS`, and `RETRY_TIMES` move into the per-source config. A slow government site gets delay=3s/concurrency=2; a faster source gets delay=0.5s/concurrency=16.
