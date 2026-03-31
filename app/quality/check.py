"""Quality check stage.

Validates the landing and processed collections for a given date range.
Exits with code 1 if any check fails — Airflow treats that as a task failure
and will retry according to the DAG's retry policy.

Zero records for a date range is considered valid (no decisions published
that month is a normal business scenario).  The checks only apply when
records exist.

Checks performed:
  1. All landing records have the required fields.
  2. All landing records have a corresponding processed record.
  3. Record failure rate is below the configured threshold.
"""

import argparse
import sys

from app.common.config import get_config
from app.common.logging_utils import get_json_logger, log_json
from app.storage.mongo_client import MongoMetadataClient

# A partition is considered unhealthy if more than this fraction of records failed
FAILURE_RATE_THRESHOLD = 0.20

REQUIRED_LANDING_FIELDS = [
    "identifier",
    "source",
    "title",
    "published_date_iso",
    "body",
    "file_type",
    "object_storage_path",
    "file_hash",
]


def main(start_date: str, end_date: str) -> None:
    config = get_config()
    logger = get_json_logger("wrc_quality", config.log_dir, "quality.jsonl")
    mongo = MongoMetadataClient()

    landing_docs = mongo.fetch_landing_by_date_range(start_date, end_date)
    processed_docs = mongo.fetch_processed_by_date_range(start_date, end_date)

    processed_ids = {d["identifier"] for d in processed_docs}

    failures: list[dict] = []
    total = len(landing_docs)

    # --- No records: valid scenario, nothing to validate ---
    if not landing_docs:
        log_json(
            logger, "info", "quality_check_summary",
            start_date=start_date,
            end_date=end_date,
            total_landing=0,
            total_processed=0,
            missing_fields_count=0,
            not_processed_count=0,
            failure_rate=0.0,
            threshold=FAILURE_RATE_THRESHOLD,
            passed=True,
            note="no records for this date range — skipping field checks",
        )
        print("Quality check PASSED: no records for this date range (valid)")
        return

    # --- Check 2: required fields present ---
    missing_fields_count = 0
    for doc in landing_docs:
        identifier = doc.get("identifier", "<unknown>")
        missing = [f for f in REQUIRED_LANDING_FIELDS if not doc.get(f)]
        if missing:
            missing_fields_count += 1
            failures.append({
                "check": "missing_required_fields",
                "identifier": identifier,
                "missing_fields": missing,
            })
            log_json(
                logger, "warning", "quality_record_issue",
                check="missing_required_fields",
                identifier=identifier,
                missing_fields=missing,
            )

    # --- Check 3: every landing record has a processed counterpart ---
    not_processed_count = 0
    for doc in landing_docs:
        identifier = doc.get("identifier", "<unknown>")
        if identifier not in processed_ids:
            not_processed_count += 1
            failures.append({
                "check": "not_processed",
                "identifier": identifier,
            })
            log_json(
                logger, "warning", "quality_record_issue",
                check="not_processed",
                identifier=identifier,
            )

    # --- Check 4: failure rate threshold ---
    failure_rate = len(failures) / total if total else 0.0
    quality_passed = failure_rate <= FAILURE_RATE_THRESHOLD

    log_json(
        logger, "info" if quality_passed else "error", "quality_check_summary",
        start_date=start_date,
        end_date=end_date,
        total_landing=total,
        total_processed=len(processed_docs),
        missing_fields_count=missing_fields_count,
        not_processed_count=not_processed_count,
        failure_rate=round(failure_rate, 4),
        threshold=FAILURE_RATE_THRESHOLD,
        passed=quality_passed,
    )

    if not quality_passed:
        print(
            f"Quality check FAILED: failure_rate={failure_rate:.1%} exceeds "
            f"threshold={FAILURE_RATE_THRESHOLD:.1%} "
            f"({len(failures)}/{total} records)",
            file=sys.stderr,
        )
        sys.exit(1)

    print(
        f"Quality check PASSED: {total - len(failures)}/{total} records healthy "
        f"(failure_rate={failure_rate:.1%})"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    args = parser.parse_args()
    main(args.start_date, args.end_date)
