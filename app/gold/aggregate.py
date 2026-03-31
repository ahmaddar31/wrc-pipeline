"""Gold aggregation — monthly statistics data mart.

Runs a MongoDB aggregation pipeline over `gold_decisions` for the given
date range and writes one summary document per (body, month) pair into
the `monthly_stats` collection.

Each document looks like:
{
    "body":             "Workplace Relations Commission",
    "body_id":          "wrc",
    "year_month":       "2024-03",
    "total_decisions":  42,
    "pdf_count":        30,
    "html_count":       12,
    "avg_word_count":   1850.5,
    "computed_at":      "2024-04-01T00:00:00Z"
}
"""

import argparse
from datetime import datetime

from app.common.config import get_config
from app.common.logging_utils import get_json_logger, log_json
from app.storage.mongo_client import MongoMetadataClient


def main(start_date: str, end_date: str) -> None:
    config = get_config()
    logger = get_json_logger("wrc_aggregate", config.log_dir, "gold.jsonl")
    mongo = MongoMetadataClient()

    pipeline = [
        # Filter to the requested date window
        {"$match": {
            "published_date_iso": {"$gte": start_date, "$lte": end_date},
        }},
        # Extract "YYYY-MM" from the ISO date string
        {"$addFields": {
            "year_month": {"$substr": ["$published_date_iso", 0, 7]},
        }},
        # Aggregate per body per month
        {"$group": {
            "_id": {"body_id": "$body_id", "year_month": "$year_month"},
            "body":             {"$first": "$body"},
            "body_id":          {"$first": "$body_id"},
            "year_month":       {"$first": "$year_month"},
            "total_decisions":  {"$sum": 1},
            "pdf_count":        {"$sum": {"$cond": [{"$eq": ["$file_type", "pdf"]},  1, 0]}},
            "html_count":       {"$sum": {"$cond": [{"$eq": ["$file_type", "html"]}, 1, 0]}},
            "avg_word_count":   {"$avg": "$word_count"},
        }},
        {"$sort": {"year_month": 1, "body_id": 1}},
    ]

    results = list(mongo.gold_collection.aggregate(pipeline))
    now = datetime.utcnow().isoformat() + "Z"

    for row in results:
        row.pop("_id", None)
        row["avg_word_count"] = round(row.get("avg_word_count") or 0, 1)
        row["computed_at"] = now
        mongo.upsert_monthly_stat(row)
        log_json(
            logger, "info", "monthly_stat_upserted",
            body_id=row["body_id"],
            year_month=row["year_month"],
            total_decisions=row["total_decisions"],
            pdf_count=row["pdf_count"],
            html_count=row["html_count"],
            avg_word_count=row["avg_word_count"],
        )

    log_json(
        logger, "info", "aggregation_finished",
        start_date=start_date,
        end_date=end_date,
        partitions_written=len(results),
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    args = parser.parse_args()
    main(args.start_date, args.end_date)
