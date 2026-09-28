"""
Mongo Serving — Wikipedia Pipeline (Phase 5)

Streams the Gold Delta table into MongoDB with idempotent upserts keyed by
``story_id:update_seq``, so replays and re-emitted rows never duplicate news.
"""

import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Iterator, List

from pymongo import ASCENDING, DESCENDING, MongoClient, UpdateOne
from pymongo.errors import PyMongoError
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit

from common.spark_common import MINIO_BUCKET, create_spark_session, wait_for_delta_source

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("wiki-serving")

GOLD_PATH = f"s3a://{MINIO_BUCKET}/gold/wiki_news"
SERVING_CHECKPOINT_PATH = f"s3a://{MINIO_BUCKET}/serving/_checkpoint"

MONGO_HOST = os.getenv("MONGO_HOST", "mongodb")
MONGO_PORT = int(os.getenv("MONGO_PORT", "27017"))
MONGO_USER = os.getenv("MONGO_INITDB_ROOT_USERNAME", "wikipedia")
MONGO_PASSWORD = os.getenv("MONGO_INITDB_ROOT_PASSWORD", "wikipedia123")
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "wikipedia")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "news")

SERVING_TRIGGER_INTERVAL = os.getenv("SERVING_TRIGGER_INTERVAL", "30 seconds")
SERVING_STARTING_VERSION = os.getenv("SERVING_STARTING_VERSION", "latest")
# Maximum documents per MongoDB bulk write (larger micro-batches are split, never truncated)
SERVING_BATCH_LIMIT = int(os.getenv("SERVING_BATCH_LIMIT", "200"))
SERVING_BOOTSTRAP_ENABLED = os.getenv("SERVING_BOOTSTRAP_ENABLED", "true").lower() == "true"
SERVING_BOOTSTRAP_MAX_RECORDS = int(os.getenv("SERVING_BOOTSTRAP_MAX_RECORDS", "5000"))


INDEXES_CREATED = False


def build_mongo_uri() -> str:
    return f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/?authSource=admin"


def parse_iso_datetime(value: Any) -> datetime:
    if value is None:
        return datetime.now(timezone.utc)
    value_str = str(value)
    try:
        return datetime.fromisoformat(value_str.replace("Z", "+00:00"))
    except ValueError:
        return datetime.now(timezone.utc)


def to_document(row_dict: Dict[str, Any]) -> Dict[str, Any]:
    story_id = row_dict.get("story_id") or "unknown-story"
    update_seq = int(row_dict.get("update_seq") or 1)
    doc_id = f"{story_id}:{update_seq}"

    return {
        "_id": doc_id,
        "doc_type": "wiki_news",
        "story_id": story_id,
        "update_seq": update_seq,
        "is_live_event": bool(row_dict.get("is_live_event", False)),
        "is_update": bool(row_dict.get("is_update", False)),
        "topic_term": row_dict.get("topic_term"),
        "topic_label": row_dict.get("topic_label"),
        "topic_event_count": int(row_dict.get("topic_event_count") or 0),
        "headline": row_dict.get("headline"),
        "summary": row_dict.get("summary"),
        "tags": row_dict.get("tags") or [],
        "title": row_dict.get("title"),
        "comment": row_dict.get("comment"),
        "domain": row_dict.get("domain"),
        "editor_user": row_dict.get("editor_user"),
        "title_url": row_dict.get("title_url"),
        "article_uri": row_dict.get("article_uri"),
        "event_id": row_dict.get("event_id"),
        "event_meta_id": row_dict.get("event_meta_id"),
        "event_ts": parse_iso_datetime(row_dict.get("event_ts")),
        "silver_ts": parse_iso_datetime(row_dict.get("silver_ts")),
        "gold_ts": parse_iso_datetime(row_dict.get("gold_ts")),
        "published_at": parse_iso_datetime(row_dict.get("published_at")),
        "updated_at": parse_iso_datetime(row_dict.get("updated_at")),
        "timestamp": parse_iso_datetime(row_dict.get("gold_ts")),
        "inference_ok": bool(row_dict.get("inference_ok", False)),
        "inference_error": row_dict.get("inference_error"),
        "dedup_score": float(row_dict.get("dedup_score") or 0.0),
        "duplicate_of_gold_ts": row_dict.get("duplicate_of_gold_ts"),
        "source_raw_json": row_dict.get("source_raw_json"),
    }


def ensure_indexes(collection) -> None:
    collection.create_index([("timestamp", DESCENDING)], name="idx_timestamp_desc")
    collection.create_index([("story_id", ASCENDING), ("update_seq", ASCENDING)], unique=True, name="idx_story_update")
    collection.create_index([("topic_term", ASCENDING), ("timestamp", DESCENDING)], name="idx_topic_timestamp")
    collection.create_index([("topic_label", ASCENDING), ("timestamp", DESCENDING)], name="idx_topic_label_timestamp")
    collection.create_index(
        [
            ("headline", "text"),
            ("summary", "text"),
            ("tags", "text"),
            ("topic_term", "text"),
        ],
        name="idx_news_text",
        default_language="english",
    )


def write_documents_to_mongo(rows: List[Dict[str, Any]], source: str, batch_id: Any) -> None:
    global INDEXES_CREATED

    if not rows:
        logger.info("source=%s batch=%s total=0", source, batch_id)
        return

    mongo_uri = build_mongo_uri()
    with MongoClient(mongo_uri, serverSelectionTimeoutMS=5000) as client:
        db = client[MONGO_DATABASE]
        collection = db[MONGO_COLLECTION]

        if not INDEXES_CREATED:
            ensure_indexes(collection)
            INDEXES_CREATED = True

        operations: List[UpdateOne] = []
        for row_dict in rows:
            document = to_document(row_dict)
            operations.append(
                UpdateOne(
                    {"_id": document["_id"]},
                    {
                        "$set": document,
                        "$setOnInsert": {
                            "created_at": datetime.now(timezone.utc),
                        },
                    },
                    upsert=True,
                )
            )

        result = collection.bulk_write(operations, ordered=False)
        logger.info(
            "source=%s batch=%s sent=%s upserted=%s modified=%s matched=%s",
            source,
            batch_id,
            len(rows),
            len(result.upserted_ids),
            result.modified_count,
            result.matched_count,
        )


def bootstrap_from_gold(spark: SparkSession) -> None:
    if not SERVING_BOOTSTRAP_ENABLED:
        return

    try:
        bootstrap_df = (
            spark.read.format("delta")
            .load(GOLD_PATH)
            .orderBy(col("gold_ts").desc())
            .limit(SERVING_BOOTSTRAP_MAX_RECORDS)
        )
    except Exception as exc:
        logger.info("Bootstrap skipped (Gold not available yet): %s", exc)
        return

    rows = [row.asDict(recursive=True) for row in bootstrap_df.collect()]
    if not rows:
        logger.info("Bootstrap without rows to load")
        return

    try:
        write_documents_to_mongo(rows, source="bootstrap", batch_id="init")
    except PyMongoError as exc:
        logger.error("Bootstrap Mongo error=%s", exc)
        raise


def iter_chunks(rows: Iterable[Dict[str, Any]], size: int) -> Iterator[List[Dict[str, Any]]]:
    """Group a stream of rows into bulk-write sized chunks without materializing it."""
    size = max(size, 1)
    chunk: List[Dict[str, Any]] = []
    for row in rows:
        chunk.append(row)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def write_batch_to_mongo(batch_df: DataFrame, batch_id: int) -> None:
    # toLocalIterator pulls one partition at a time, so driver memory is bounded by a
    # partition plus one chunk, not by the micro-batch (which can be large after Gold
    # rewrites files and ignoreChanges re-emits them).
    rows = (row.asDict(recursive=True) for row in batch_df.orderBy("gold_ts").toLocalIterator())

    # Every row must reach MongoDB: the checkpoint advances once this batch returns,
    # so anything left out here would be lost for good.
    written = 0
    try:
        for chunk in iter_chunks(rows, SERVING_BATCH_LIMIT):
            write_documents_to_mongo(chunk, source="stream", batch_id=batch_id)
            written += len(chunk)
    except PyMongoError as exc:
        logger.error("batch=%s written_before_error=%s error_mongo=%s", batch_id, written, exc)
        raise

    if written == 0:
        logger.info("batch=%s total=0 (no new gold events)", batch_id)
    else:
        logger.info("batch=%s incoming=%s", batch_id, written)


def read_gold_stream(
    spark: SparkSession, path: str = GOLD_PATH, starting_version: str = SERVING_STARTING_VERSION
) -> DataFrame:
    """
    Streaming read of the Gold table.

    Gold rewrites rows when it closes stale live stories (UPDATE ... SET is_live_event = false).
    By default a Delta stream fails on any non-append commit (DELTA_SOURCE_TABLE_IGNORE_CHANGES);
    ignoreChanges re-emits the rewritten rows instead, and the idempotent upserts apply the new
    state to MongoDB. skipChangeCommits would keep the stream alive but silently drop the update.
    """
    return (
        spark.readStream.format("delta")
        .option("startingVersion", starting_version)
        .option("ignoreChanges", "true")
        .load(path)
    )


def main() -> None:
    spark = create_spark_session("WikiMongoServing")
    spark.sparkContext.setLogLevel("WARN")

    logger.info("Starting Mongo serving from %s", GOLD_PATH)
    bootstrap_from_gold(spark)
    wait_for_delta_source(spark, GOLD_PATH, "Gold")

    gold_stream_df = read_gold_stream(spark)
    if "topic_label" not in gold_stream_df.columns:
        gold_stream_df = gold_stream_df.withColumn("topic_label", lit(None).cast("string"))

    selected_df = gold_stream_df.select(
        "topic_term",
        "topic_label",
        "topic_event_count",
        "event_id",
        "event_meta_id",
        "domain",
        "article_uri",
        "title",
        "comment",
        "editor_user",
        "title_url",
        "event_ts",
        "silver_ts",
        "headline",
        "summary",
        "tags",
        "inference_ok",
        "inference_error",
        "dedup_score",
        "is_update",
        "duplicate_of_gold_ts",
        "story_id",
        "published_at",
        "updated_at",
        "update_seq",
        "is_live_event",
        "gold_ts",
        "source_raw_json",
    )

    query = (
        selected_df.writeStream.foreachBatch(write_batch_to_mongo)
        .option("checkpointLocation", SERVING_CHECKPOINT_PATH)
        .trigger(processingTime=SERVING_TRIGGER_INTERVAL)
        .start()
    )

    logger.info(
        "Mongo serving started → db=%s collection=%s trigger=%s checkpoint=%s",
        MONGO_DATABASE,
        MONGO_COLLECTION,
        SERVING_TRIGGER_INTERVAL,
        SERVING_CHECKPOINT_PATH,
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
