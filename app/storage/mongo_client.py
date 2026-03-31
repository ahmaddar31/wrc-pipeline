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

