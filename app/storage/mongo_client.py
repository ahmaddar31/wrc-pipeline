from __future__ import annotations

from datetime import datetime
from pymongo import MongoClient, ASCENDING
from pymongo.collection import Collection

from app.common.config import get_config


class MongoMetadataClient:
    def __init__(self) -> None:
        config = get_config()
        self.client = MongoClient(config.mongo_uri)
        self.db = self.client[config.mongo_db]
        self.landing_collection: Collection = self.db[config.mongo_landing_collection]
        self.processed_collection: Collection = self.db[config.mongo_processed_collection]
        self.gold_collection: Collection = self.db[config.mongo_gold_collection]
        self.stats_collection: Collection = self.db[config.mongo_stats_collection]
        self._ensure_indexes()

    def _ensure_indexes(self) -> None:
        self.landing_collection.create_index(
            [("source", ASCENDING), ("identifier", ASCENDING)],
            unique=True,
            name="uq_source_identifier_landing",
        )
        self.processed_collection.create_index(
            [("source", ASCENDING), ("identifier", ASCENDING)],
            unique=True,
            name="uq_source_identifier_processed",
        )
        self.gold_collection.create_index(
            [("source", ASCENDING), ("identifier", ASCENDING)],
            unique=True,
            name="uq_source_identifier_gold",
        )
        self.gold_collection.create_index(
            [("published_date_iso", ASCENDING)],
            name="idx_gold_published_date",
        )
        self.gold_collection.create_index(
            [("body_id", ASCENDING)],
            name="idx_gold_body_id",
        )
        self.stats_collection.create_index(
            [("body_id", ASCENDING), ("year_month", ASCENDING)],
            unique=True,
            name="uq_stats_body_month",
        )

    def upsert_landing_metadata(self, document: dict) -> None:
        now = datetime.utcnow().isoformat() + "Z"
        document["last_seen_at"] = now
        self.landing_collection.update_one(
            {"source": document["source"], "identifier": document["identifier"]},
            {
                "$set": document,
                "$setOnInsert": {"created_at": now},
            },
            upsert=True,
        )

    def get_landing_by_identifier(self, source: str, identifier: str) -> dict | None:
        return self.landing_collection.find_one({"source": source, "identifier": identifier})

    def fetch_landing_by_date_range(self, start_date: str, end_date: str) -> list[dict]:
        return list(
            self.landing_collection.find(
                {
                    "published_date_iso": {
                        "$gte": start_date,
                        "$lte": end_date,
                    }
                }
            )
        )

    def upsert_processed_metadata(self, document: dict) -> None:
        now = datetime.utcnow().isoformat() + "Z"
        document["last_seen_at"] = now
        self.processed_collection.update_one(
            {"source": document["source"], "identifier": document["identifier"]},
            {
                "$set": document,
                "$setOnInsert": {"created_at": now},
            },
            upsert=True,
        )

    def fetch_processed_by_date_range(self, start_date: str, end_date: str) -> list[dict]:
        return list(
            self.processed_collection.find(
                {
                    "published_date_iso": {
                        "$gte": start_date,
                        "$lte": end_date,
                    }
                }
            )
        )

    def upsert_monthly_stat(self, document: dict) -> None:
        self.stats_collection.update_one(
            {"body_id": document["body_id"], "year_month": document["year_month"]},
            {"$set": document},
            upsert=True,
        )

    def upsert_gold_decision(self, document: dict) -> None:
        now = datetime.utcnow().isoformat() + "Z"
        document["gold_processed_at"] = now
        self.gold_collection.update_one(
            {"source": document["source"], "identifier": document["identifier"]},
            {
                "$set": document,
                "$setOnInsert": {"created_at": now},
            },
            upsert=True,
        )