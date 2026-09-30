"""Silver Processing — Wikipedia Pipeline (Phase 3)

Cleans Bronze events and selects the article bursts that deserve a news story.

Selection uses the structured fields of the Wikimedia ``recentchange`` event
(``wiki``, ``type``, ``namespace``, ``length``, ``revision``) and ranks articles by
how many *different* people are editing them inside the lookback window: a
sudden crowd of editors on one article is the classic breaking-news signal.
"""

import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Mapping, Optional, Tuple

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql.functions import (
    coalesce,
    col,
    count,
    countDistinct,
    current_timestamp,
    desc,
    expr,
    first,
    from_json,
    from_unixtime,
    greatest,
    lit,
    lower,
    regexp_extract,
    to_timestamp,
    trim,
    when,
)
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.types import BooleanType, DoubleType, LongType, StringType, StructField, StructType

from common.editorial_common import (
    ChangeFilter,
    burst_score,
    looks_like_generic_topic,
    looks_like_low_signal_topic,
    rank_bursts,
    sanitize_topic_label,
)
from common.editorial_spark import with_editorial_signals
from common.spark_common import MINIO_BUCKET, create_spark_session, wait_for_delta_source

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("wiki-silver")

BRONZE_PATH = f"s3a://{MINIO_BUCKET}/bronze/wiki_raw"
SILVER_OUTPUT_PATH = f"s3a://{MINIO_BUCKET}/silver/wiki_clean"
SILVER_TOPICS_PATH = f"s3a://{MINIO_BUCKET}/silver/wiki_topics"
SILVER_CHECKPOINT_PATH = f"s3a://{MINIO_BUCKET}/silver/_checkpoint"
SILVER_METRICS_PATH = f"s3a://{MINIO_BUCKET}/silver/_metrics"

TRIGGER_INTERVAL = os.getenv("SILVER_TRIGGER_INTERVAL", "30 seconds")
DEFAULT_REVERT_SIGNAL_REGEX = r".*\b(undid revision|revert(?:ed|ing)? .*edit(?:s)? by)\b.*"


@dataclass(frozen=True)
class SelectionConfig:
    """Every knob of the news selection, so jobs, tests and offline tools share one definition."""

    change_filter: ChangeFilter = field(default_factory=ChangeFilter)
    revert_regex: str = DEFAULT_REVERT_SIGNAL_REGEX
    hold_minutes: int = 5
    lookback_minutes: int = 30
    top_n: int = 5
    min_edits: int = 5
    min_editors: int = 3
    sample_size: int = 12

    @property
    def candidate_pool_size(self) -> int:
        return max(self.top_n * 3, 12)

    @classmethod
    def from_env(cls, env: Optional[Mapping[str, str]] = None) -> "SelectionConfig":
        env = os.environ if env is None else env
        return cls(
            change_filter=ChangeFilter.from_settings(
                wikis=env.get("SILVER_ALLOWED_WIKIS", "enwiki"),
                types=env.get("SILVER_ALLOWED_TYPES", "edit,new"),
                namespaces=env.get("SILVER_ALLOWED_NAMESPACES", "0"),
            ),
            revert_regex=env.get("SILVER_REVERT_SIGNAL_REGEX", DEFAULT_REVERT_SIGNAL_REGEX).strip(),
            hold_minutes=int(env.get("SILVER_EVENT_HOLD_MINUTES", "5")),
            lookback_minutes=int(env.get("SILVER_TOPIC_LOOKBACK_MINUTES", "30")),
            top_n=int(env.get("SILVER_TOPIC_TOP_N", "5")),
            min_edits=int(env.get("SILVER_TOPIC_MIN_EDITS", "5")),
            min_editors=int(env.get("SILVER_TOPIC_MIN_EDITORS", "3")),
            sample_size=int(env.get("SILVER_TOPIC_SAMPLE_SIZE", "12")),
        )


SELECTION = SelectionConfig.from_env()

_REVISION_PAIR = StructType([StructField("old", LongType(), True), StructField("new", LongType(), True)])

WIKI_CHANGE_SCHEMA = StructType(
    [
        StructField("id", LongType(), True),
        StructField("type", StringType(), True),
        StructField("namespace", LongType(), True),
        StructField("wiki", StringType(), True),
        StructField("server_name", StringType(), True),
        StructField("title", StringType(), True),
        StructField("comment", StringType(), True),
        StructField("user", StringType(), True),
        StructField("bot", BooleanType(), True),
        StructField("minor", BooleanType(), True),
        StructField("timestamp", LongType(), True),
        StructField("title_url", StringType(), True),
        StructField("length", _REVISION_PAIR, True),
        StructField("revision", _REVISION_PAIR, True),
        StructField(
            "meta",
            StructType(
                [
                    StructField("id", StringType(), True),
                    StructField("domain", StringType(), True),
                    StructField("uri", StringType(), True),
                    StructField("dt", StringType(), True),
                ]
            ),
            True,
        ),
    ]
)

TOPIC_SCHEMA = StructType(
    [
        StructField("topic_term", StringType(), False),
        StructField("topic_key", StringType(), False),
        StructField("topic_mode", StringType(), False),
        StructField("topic_label", StringType(), False),
        StructField("topic_event_count", LongType(), False),
        StructField("topic_editor_count", LongType(), False),
        StructField("topic_bytes_added", LongType(), False),
        StructField("topic_score", DoubleType(), False),
        StructField("event_id", LongType(), True),
        StructField("event_meta_id", StringType(), True),
        StructField("wiki", StringType(), True),
        StructField("server_name", StringType(), True),
        StructField("domain", StringType(), True),
        StructField("article_uri", StringType(), True),
        StructField("title", StringType(), True),
        StructField("comment", StringType(), True),
        StructField("editor_user", StringType(), True),
        StructField("title_url", StringType(), True),
        StructField("event_ts", StringType(), True),
        StructField("silver_ts", StringType(), True),
        StructField("samples_json", StringType(), False),
        StructField("silver_topic_ts", StringType(), False),
        StructField("source_raw_json", StringType(), True),
    ]
)


def change_filter_condition(change_filter: ChangeFilter) -> Column:
    """Spark twin of ``ChangeFilter.accepts``: no bots, no minor edits, allowed wiki/type/namespace."""
    condition = (~coalesce(col("bot"), lit(False))) & (~coalesce(col("minor"), lit(False)))
    if change_filter.wikis:
        condition = condition & col("wiki").isin(sorted(change_filter.wikis))
    if change_filter.types:
        condition = condition & col("change_type").isin(sorted(change_filter.types))
    if change_filter.namespaces:
        condition = condition & col("namespace").isin(sorted(change_filter.namespaces))
    return condition


def transform_to_silver(bronze_batch_df: DataFrame, config: SelectionConfig = SELECTION) -> DataFrame:
    parsed_df = bronze_batch_df.withColumn("event", from_json(col("raw_json"), WIKI_CHANGE_SCHEMA))
    selected_df = parsed_df.select(
        col("event.id").alias("event_id"),
        col("event.meta.id").alias("event_meta_id"),
        col("event.type").alias("change_type"),
        col("event.namespace").alias("namespace"),
        col("event.wiki").alias("wiki"),
        col("event.server_name").alias("server_name"),
        col("event.meta.domain").alias("domain"),
        col("event.meta.uri").alias("article_uri"),
        col("event.title").alias("title"),
        col("event.comment").alias("comment"),
        col("event.user").alias("editor_user"),
        col("event.bot").alias("bot"),
        col("event.minor").alias("minor"),
        col("event.timestamp").alias("event_unix_ts"),
        col("event.title_url").alias("title_url"),
        col("event.revision.old").alias("rev_old"),
        col("event.revision.new").alias("rev_new"),
        (coalesce(col("event.length.new"), lit(0)) - coalesce(col("event.length.old"), lit(0))).alias("byte_delta"),
        col("raw_json"),
        col("kafka_timestamp"),
        col("ingestion_ts"),
    ).filter(change_filter_condition(config.change_filter))

    revert_regex = config.revert_regex
    normalized_df = (
        selected_df.withColumn("title", trim(col("title")))
        .withColumn("comment", trim(col("comment")))
        .withColumn("title_normalized", lower(trim(col("title"))))
        .withColumn("comment_normalized", lower(trim(col("comment"))))
        .withColumn("event_ts", to_timestamp(from_unixtime(col("event_unix_ts"))))
        .withColumn(
            "revert_signal_term",
            lit("")
            if not revert_regex
            else regexp_extract(coalesce(col("comment_normalized"), lit("")), revert_regex, 1),
        )
        .withColumn("is_revert_signal", lit(False) if not revert_regex else (col("revert_signal_term") != lit("")))
        .withColumn("silver_ts", current_timestamp())
    )
    return analyze_quality(with_editorial_signals(normalized_df), config)


def analyze_quality(df: DataFrame, config: SelectionConfig = SELECTION) -> DataFrame:
    return (
        df.withColumn(
            "moderation_status",
            when(col("is_revert_signal"), lit("revert_signal"))
            .when(col("title_is_namespace"), lit("namespace"))
            .when(col("title_is_numeric"), lit("numeric"))
            .when(col("is_low_signal_topic"), lit("low_signal_topic"))
            .when(col("has_editorial_noise"), lit("technical_noise"))
            .when(col("is_editorial_topic_candidate"), lit("publishable"))
            .when(col("title_has_foreign_script"), lit("foreign_script"))
            .otherwise(lit("weak_context")),
        )
        .withColumn(
            "quality_score",
            when(col("is_revert_signal"), lit(0))
            .when(col("title_is_namespace") | col("title_is_numeric"), lit(10))
            .when(col("is_low_signal_topic"), lit(15))
            .when(col("has_editorial_noise"), lit(35))
            .when(col("is_editorial_topic_candidate"), lit(100))
            .when(col("title_has_foreign_script"), lit(80))
            .otherwise(lit(60)),
        )
        .withColumn(
            "is_publishable_candidate",
            col("event_ts").isNotNull()
            & coalesce(col("is_editorial_topic_candidate"), lit(False))
            & (~coalesce(col("is_revert_signal"), lit(False))),
        )
        .withColumn(
            "candidate_ready_ts",
            expr(f"event_ts + INTERVAL {int(config.hold_minutes)} MINUTES"),
        )
    )


def publishable_events(
    silver_df: DataFrame, now_utc: datetime, config: SelectionConfig = SELECTION
) -> Tuple[DataFrame, int]:
    """
    Events that matured for ``hold_minutes`` inside the lookback window and were not
    reverted meanwhile. Returns the events and how many were dropped as reverted.
    """
    lookback_minutes = max(config.lookback_minutes, config.hold_minutes)
    lookback_start_iso = (now_utc - timedelta(minutes=lookback_minutes)).isoformat()
    windowed_df = silver_df.filter(col("event_ts").isNotNull()).filter(
        col("event_ts") >= lit(lookback_start_iso).cast("timestamp")
    )

    candidates_df = windowed_df.filter(coalesce(col("is_publishable_candidate"), lit(False))).filter(
        col("candidate_ready_ts") <= lit(now_utc.isoformat()).cast("timestamp")
    )
    revert_signals_df = windowed_df.filter(coalesce(col("is_revert_signal"), lit(False))).select(
        col("domain").alias("revert_domain"),
        col("title_normalized").alias("revert_title_normalized"),
        col("event_ts").alias("revert_event_ts"),
    )
    tainted_event_ids_df = (
        candidates_df.alias("candidate")
        .join(
            revert_signals_df.alias("revert"),
            on=(col("candidate.domain") == col("revert.revert_domain"))
            & (col("candidate.title_normalized") == col("revert.revert_title_normalized"))
            & (col("revert.revert_event_ts") >= col("candidate.event_ts"))
            & (col("revert.revert_event_ts") <= col("candidate.candidate_ready_ts")),
            how="inner",
        )
        .select(col("candidate.event_id").alias("event_id"))
        .distinct()
    )
    tainted_count = tainted_event_ids_df.count()
    return candidates_df.join(tainted_event_ids_df, on="event_id", how="left_anti"), tainted_count


def load_publishable_events(
    spark: SparkSession, now_utc: datetime, config: SelectionConfig = SELECTION
) -> Tuple[DataFrame, int]:
    return publishable_events(spark.read.format("delta").load(SILVER_OUTPUT_PATH), now_utc, config)


def detect_hot_topic_terms(df: DataFrame, config: SelectionConfig = SELECTION) -> List[Dict[str, Any]]:
    """
    Articles edited by at least ``min_editors`` different people (and ``min_edits``
    edits) in the window, ranked by ``burst_score``.
    """
    bursts = (
        df.filter(coalesce(col("is_editorial_topic_candidate"), lit(False)))
        .groupBy("editorial_topic_key")
        .agg(
            first("editorial_topic_label", ignorenulls=True).alias("topic_label"),
            count("event_id").alias("edits"),
            countDistinct("editor_user").alias("editors"),
            spark_sum(greatest(coalesce(col("byte_delta"), lit(0)), lit(0))).alias("bytes_added"),
        )
        .filter((col("edits") >= config.min_edits) & (col("editors") >= config.min_editors))
        .orderBy(desc("editors"), desc("edits"), col("editorial_topic_key"))
        .limit(config.candidate_pool_size)
        .collect()
    )

    topics: List[Dict[str, Any]] = []
    for row in bursts:
        topic_key = row["editorial_topic_key"]
        topic_label = (row["topic_label"] or "").strip()
        if (
            not topic_key
            or not topic_label
            or looks_like_low_signal_topic(topic_key)
            or looks_like_low_signal_topic(topic_label)
        ):
            continue
        edits, editors, bytes_added = int(row["edits"]), int(row["editors"]), int(row["bytes_added"] or 0)
        topics.append(
            {
                "label": topic_label,
                "key": topic_key,
                "mode": "article_burst",
                "count": edits,
                "editors": editors,
                "bytes_added": bytes_added,
                "score": burst_score(topic_label, editors, edits, bytes_added),
            }
        )
    return rank_bursts(topics)[: config.top_n]


SAMPLE_COLUMNS = (
    "event_id",
    "event_meta_id",
    "wiki",
    "server_name",
    "domain",
    "article_uri",
    "title",
    "comment",
    "editor_user",
    "title_url",
    "rev_old",
    "rev_new",
    "byte_delta",
    "event_ts",
    "silver_ts",
    "raw_json",
)


def _optional_int(value: Any) -> Optional[int]:
    return int(value) if value is not None else None


def build_topic_records(
    publishable_df: DataFrame, now_utc: datetime, config: SelectionConfig = SELECTION
) -> List[Dict[str, Any]]:
    topic_records: List[Dict[str, Any]] = []
    for topic in detect_hot_topic_terms(publishable_df, config):
        sample_rows = (
            publishable_df.filter(col("editorial_topic_key") == lit(topic["key"]))
            .orderBy(col("event_ts").desc())
            .limit(config.sample_size)
            .select(*SAMPLE_COLUMNS)
            .collect()
        )
        if not sample_rows:
            continue

        samples = [
            {
                "title": row["title"] or "",
                "comment": row["comment"] or "",
                "domain": row["domain"] or "",
                "server_name": row["server_name"] or row["domain"] or "",
                "rev_old": _optional_int(row["rev_old"]),
                "rev_new": _optional_int(row["rev_new"]),
                "byte_delta": int(row["byte_delta"] or 0),
            }
            for row in sample_rows
        ]
        representative = sample_rows[0]
        topic_term = topic["label"]
        topic_label = sanitize_topic_label(topic_term, [], representative["domain"])
        if looks_like_low_signal_topic(topic_label):
            continue

        generic_topic_penalty = (
            1 if looks_like_generic_topic(topic_term) or looks_like_generic_topic(topic_label) else 0
        )
        topic_records.append(
            {
                "topic_term": topic_term,
                "topic_key": topic["key"],
                "topic_mode": topic["mode"],
                "topic_label": topic_label,
                "topic_event_count": max(int(topic["count"]) - generic_topic_penalty, 1),
                "topic_editor_count": int(topic["editors"]),
                "topic_bytes_added": int(topic["bytes_added"]),
                "topic_score": float(topic["score"]),
                "event_id": representative["event_id"],
                "event_meta_id": representative["event_meta_id"],
                "wiki": representative["wiki"],
                "server_name": representative["server_name"],
                "domain": representative["domain"],
                "article_uri": representative["article_uri"],
                "title": representative["title"],
                "comment": representative["comment"],
                "editor_user": representative["editor_user"],
                "title_url": representative["title_url"],
                "event_ts": str(representative["event_ts"]) if representative["event_ts"] is not None else None,
                "silver_ts": str(representative["silver_ts"]) if representative["silver_ts"] is not None else None,
                "samples_json": json.dumps(samples, ensure_ascii=False),
                "silver_topic_ts": now_utc.isoformat(),
                "source_raw_json": representative["raw_json"],
            }
        )
    return topic_records


def build_topic_candidates(
    spark: SparkSession, now_utc: datetime, config: SelectionConfig = SELECTION
) -> Tuple[List[Dict[str, Any]], int, int]:
    publishable_df, tainted_count = load_publishable_events(spark, now_utc, config)
    publishable_count = publishable_df.count()
    if publishable_count == 0:
        return [], 0, tainted_count
    return build_topic_records(publishable_df, now_utc, config), publishable_count, tainted_count


def write_batch_metrics(
    spark: SparkSession,
    batch_id: int,
    total: int,
    kept: int,
    publishable: int,
    tainted: int,
    topic_candidates: int,
) -> None:
    discarded = total - kept
    discarded_pct = (discarded / total * 100.0) if total else 0.0
    processed_at = datetime.now(timezone.utc).isoformat()
    metrics_schema = StructType(
        [
            StructField("batch_id", LongType(), False),
            StructField("total_events", LongType(), False),
            StructField("kept_events", LongType(), False),
            StructField("discarded_events", LongType(), False),
            StructField("discarded_pct", DoubleType(), False),
            StructField("publishable_events", LongType(), False),
            StructField("tainted_events", LongType(), False),
            StructField("topic_candidates", LongType(), False),
            StructField("processed_at", StringType(), False),
        ]
    )
    row = (
        int(batch_id),
        int(total),
        int(kept),
        int(discarded),
        float(discarded_pct),
        int(publishable),
        int(tainted),
        int(topic_candidates),
        processed_at,
    )
    spark.createDataFrame([row], schema=metrics_schema).write.format("delta").mode("append").save(SILVER_METRICS_PATH)


def main() -> None:
    spark = create_spark_session("WikiSilverProcessing")
    spark.sparkContext.setLogLevel("WARN")

    logger.info("Starting Silver processing from %s", BRONZE_PATH)
    wait_for_delta_source(spark, BRONZE_PATH, "Bronze")
    bronze_stream_df = spark.readStream.format("delta").load(BRONZE_PATH)

    def process_batch(batch_df: DataFrame, batch_id: int) -> None:
        total_events = batch_df.count()
        if total_events == 0:
            logger.info("batch=%s total=0 (no new events)", batch_id)
            return

        silver_batch_df = transform_to_silver(batch_df)
        kept_events = silver_batch_df.count()
        silver_batch_df.write.format("delta").mode("append").option("mergeSchema", "true").save(SILVER_OUTPUT_PATH)

        now_utc = datetime.now(timezone.utc)
        topic_records, publishable_events, tainted_events = build_topic_candidates(spark, now_utc)
        if topic_records:
            spark.createDataFrame(topic_records, schema=TOPIC_SCHEMA).write.format("delta").mode("append").option(
                "mergeSchema", "true"
            ).save(SILVER_TOPICS_PATH)

        write_batch_metrics(
            spark,
            batch_id,
            total_events,
            kept_events,
            publishable_events,
            tainted_events,
            len(topic_records),
        )
        logger.info(
            "batch=%s total=%s kept=%s publishable=%s tainted=%s topics=%s",
            batch_id,
            total_events,
            kept_events,
            publishable_events,
            tainted_events,
            len(topic_records),
        )

    query = (
        bronze_stream_df.writeStream.foreachBatch(process_batch)
        .option("checkpointLocation", SILVER_CHECKPOINT_PATH)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    logger.info(
        "Silver processing started → output=%s topics=%s metrics=%s trigger=%s",
        SILVER_OUTPUT_PATH,
        SILVER_TOPICS_PATH,
        SILVER_METRICS_PATH,
        TRIGGER_INTERVAL,
    )
    logger.info("Selection config: %s", SELECTION)
    query.awaitTermination()


if __name__ == "__main__":
    main()
