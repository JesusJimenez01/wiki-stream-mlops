"""Silver Processing — Wikipedia Pipeline (Phase 3)"""

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    coalesce,
    col,
    concat_ws,
    count,
    current_timestamp,
    desc,
    explode,
    expr,
    first,
    from_json,
    from_unixtime,
    length,
    lit,
    lower,
    regexp_extract,
    regexp_replace,
    split,
    to_timestamp,
    trim,
    when,
)
from pyspark.sql.types import BooleanType, DoubleType, LongType, StringType, StructField, StructType

from common.editorial_common import (
    looks_like_generic_topic,
    looks_like_low_signal_topic,
    sanitize_topic_label,
    topic_rank_score,
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
REVERT_SIGNAL_REGEX = os.getenv(
    "SILVER_REVERT_SIGNAL_REGEX",
    r".*\b(undid revision|revert(?:ed|ing)? .*edit(?:s)? by)\b.*",
).strip()
EVENT_HOLD_MINUTES = int(os.getenv("SILVER_EVENT_HOLD_MINUTES", "5"))
TOPIC_LOOKBACK_MINUTES = int(os.getenv("SILVER_TOPIC_LOOKBACK_MINUTES", "30"))
TOPIC_TOP_N = int(os.getenv("SILVER_TOPIC_TOP_N", "5"))
TOPIC_MIN_DOC_FREQ = int(os.getenv("SILVER_TOPIC_MIN_DOC_FREQ", "8"))
TOPIC_SAMPLE_SIZE = int(os.getenv("SILVER_TOPIC_SAMPLE_SIZE", "12"))
TOKEN_MIN_LEN = int(os.getenv("SILVER_TOKEN_MIN_LEN", "4"))
TITLE_TOPIC_MIN_DOC_FREQ = max(3, min(TOPIC_MIN_DOC_FREQ, 5))
TOPIC_CANDIDATE_POOL_SIZE = max(TOPIC_TOP_N * 3, 12)
NOISE_TOKEN_REGEX = r"^(q\d+|p\d+|special|create|property|batch|short|removed|toollabs|wbeditentity)$"
STOPWORDS = {
    word.strip().lower()
    for word in os.getenv(
        "SILVER_TOPIC_STOPWORDS",
        "wikipedia,wikidata,wikimedia,commons,article,file,edit,updated,update,category,page,using,added,user,minor,bot,with,from,that,this,para,como,donde,sobre,con,from,the,and,for,are,was,you,your,http,https,www,wiki,batch,short,removed,quickstatements,wbeditentity,toollabs,property,create",
    ).split(",")
    if word.strip()
}

WIKI_CHANGE_SCHEMA = StructType(
    [
        StructField("id", LongType(), True),
        StructField("title", StringType(), True),
        StructField("comment", StringType(), True),
        StructField("user", StringType(), True),
        StructField("bot", BooleanType(), True),
        StructField("minor", BooleanType(), True),
        StructField("timestamp", LongType(), True),
        StructField("title_url", StringType(), True),
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
        StructField("event_id", LongType(), True),
        StructField("event_meta_id", StringType(), True),
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


def transform_to_silver(bronze_batch_df: DataFrame) -> DataFrame:
    parsed_df = bronze_batch_df.withColumn("event", from_json(col("raw_json"), WIKI_CHANGE_SCHEMA))
    selected_df = parsed_df.select(
        col("event.id").alias("event_id"),
        col("event.meta.id").alias("event_meta_id"),
        col("event.meta.domain").alias("domain"),
        col("event.meta.uri").alias("article_uri"),
        col("event.title").alias("title"),
        col("event.comment").alias("comment"),
        col("event.user").alias("editor_user"),
        col("event.bot").alias("bot"),
        col("event.minor").alias("minor"),
        col("event.timestamp").alias("event_unix_ts"),
        col("event.title_url").alias("title_url"),
        col("raw_json"),
        col("kafka_timestamp"),
        col("ingestion_ts"),
    ).filter((~coalesce(col("bot"), lit(False))) & (~coalesce(col("minor"), lit(False))))

    normalized_df = (
        selected_df.withColumn("title", trim(col("title")))
        .withColumn("comment", trim(col("comment")))
        .withColumn("title_normalized", lower(trim(col("title"))))
        .withColumn("comment_normalized", lower(trim(col("comment"))))
        .withColumn("event_ts", to_timestamp(from_unixtime(col("event_unix_ts"))))
        .withColumn(
            "revert_signal_term",
            lit("")
            if not REVERT_SIGNAL_REGEX
            else regexp_extract(coalesce(col("comment_normalized"), lit("")), REVERT_SIGNAL_REGEX, 1),
        )
        .withColumn(
            "is_revert_signal", lit(False) if not REVERT_SIGNAL_REGEX else (col("revert_signal_term") != lit(""))
        )
        .withColumn("silver_ts", current_timestamp())
    )
    return analyze_quality(with_editorial_signals(normalized_df))


def analyze_quality(df: DataFrame) -> DataFrame:
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
            expr(f"event_ts + INTERVAL {EVENT_HOLD_MINUTES} MINUTES"),
        )
        .withColumn(
            "topic_text",
            lower(concat_ws(" ", coalesce(col("title"), lit("")), coalesce(col("comment"), lit("")))),
        )
    )


def load_publishable_events(spark: SparkSession, now_utc: datetime) -> Tuple[DataFrame, int]:
    lookback_minutes = max(TOPIC_LOOKBACK_MINUTES, EVENT_HOLD_MINUTES)
    lookback_start_iso = (now_utc - timedelta(minutes=lookback_minutes)).isoformat()
    silver_df = spark.read.format("delta").load(SILVER_OUTPUT_PATH)

    publishable_df = (
        silver_df.filter(col("event_ts").isNotNull())
        .filter(col("event_ts") >= lit(lookback_start_iso).cast("timestamp"))
        .filter(coalesce(col("is_publishable_candidate"), lit(False)))
        .filter(col("candidate_ready_ts") <= lit(now_utc.isoformat()).cast("timestamp"))
    )
    revert_signals_df = (
        silver_df.filter(col("event_ts").isNotNull())
        .filter(col("event_ts") >= lit(lookback_start_iso).cast("timestamp"))
        .filter(coalesce(col("is_revert_signal"), lit(False)))
        .select(
            col("domain").alias("revert_domain"),
            col("title_normalized").alias("revert_title_normalized"),
            col("event_ts").alias("revert_event_ts"),
        )
    )
    tainted_event_ids_df = (
        publishable_df.alias("candidate")
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
    return publishable_df.join(tainted_event_ids_df, on="event_id", how="left_anti"), tainted_count


def detect_hot_topic_terms(df: DataFrame) -> List[Dict[str, Any]]:
    title_candidates = (
        df.filter(coalesce(col("is_editorial_topic_candidate"), lit(False)))
        .groupBy("editorial_topic_key")
        .agg(first("editorial_topic_label", ignorenulls=True).alias("topic_label"), count("event_id").alias("count"))
        .filter(col("count") >= TITLE_TOPIC_MIN_DOC_FREQ)
        .orderBy(desc("count"), desc(length(col("topic_label"))))
        .limit(TOPIC_CANDIDATE_POOL_SIZE)
        .collect()
    )

    selected_topics: List[Dict[str, Any]] = []
    selected_keys = set()
    for row in title_candidates:
        topic_key = row["editorial_topic_key"]
        topic_label = (row["topic_label"] or "").strip()
        if (
            not topic_key
            or not topic_label
            or looks_like_low_signal_topic(topic_key)
            or looks_like_low_signal_topic(topic_label)
        ):
            continue
        selected_topics.append(
            {
                "label": topic_label,
                "count": int(row["count"]),
                "mode": "title_exact",
                "key": topic_key,
                "score": topic_rank_score(topic_label, int(row["count"]), "title_exact"),
            }
        )
        selected_keys.add(topic_key)

    selected_topics = sorted(
        selected_topics, key=lambda item: (item["score"], item["count"], len(item["label"])), reverse=True
    )
    if len(selected_topics) >= TOPIC_TOP_N:
        return selected_topics[:TOPIC_TOP_N]

    remaining_slots = max(TOPIC_CANDIDATE_POOL_SIZE - len(selected_topics), 0)

    top_terms = (
        df.filter(coalesce(col("editorial_priority"), lit(0)) >= 0)
        .select(
            col("event_id"),
            explode(
                split(regexp_replace(col("topic_text"), r"[^a-zA-Z0-9áéíóúüñçàèìòùâêîôûãõäëïöÿ\s]", " "), r"\s+")
            ).alias("token"),
        )
        .filter(col("token") != "")
        .filter(~col("token").rlike(r"^[0-9]+$"))
        .filter(~col("token").rlike(NOISE_TOKEN_REGEX))
        .filter(length(col("token")) >= TOKEN_MIN_LEN)
        .filter(~col("token").isin(list(STOPWORDS)))
        .dropDuplicates(["event_id", "token"])
        .groupBy("token")
        .count()
        .filter(col("count") >= TOPIC_MIN_DOC_FREQ)
        .orderBy(desc("count"), col("token"))
        .limit(max(remaining_slots, TOPIC_CANDIDATE_POOL_SIZE))
        .collect()
    )

    for row in top_terms:
        token = row["token"]
        if token in selected_keys or looks_like_low_signal_topic(token):
            continue
        selected_topics.append(
            {
                "label": token,
                "count": int(row["count"]),
                "mode": "token_contains",
                "key": token,
                "score": topic_rank_score(token, int(row["count"]), "token_contains"),
            }
        )

    ranked_topics = sorted(
        selected_topics,
        key=lambda item: (item["score"], item["count"], item["mode"] == "title_exact", len(item["label"])),
        reverse=True,
    )
    return ranked_topics[:TOPIC_TOP_N]


def build_topic_candidates(spark: SparkSession, now_utc: datetime) -> Tuple[List[Dict[str, Any]], int, int]:
    publishable_df, tainted_count = load_publishable_events(spark, now_utc)
    publishable_count = publishable_df.count()
    if publishable_count == 0:
        return [], 0, tainted_count

    hot_terms = detect_hot_topic_terms(publishable_df)
    if not hot_terms:
        return [], publishable_count, tainted_count

    topic_records: List[Dict[str, Any]] = []
    covered_event_ids = set()
    for topic in hot_terms:
        topic_events_df = (
            (
                publishable_df.filter(col("editorial_topic_key") == lit(topic["key"]))
                if topic["mode"] == "title_exact"
                else publishable_df.filter(col("topic_text").contains(topic["label"]))
            )
            .orderBy(col("event_ts").desc())
            .limit(TOPIC_SAMPLE_SIZE)
        )

        sample_rows = topic_events_df.select(
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
            "raw_json",
        ).collect()
        if not sample_rows:
            continue

        sample_event_ids = {int(row["event_id"]) for row in sample_rows if row["event_id"] is not None}
        overlap_ratio = (
            len(sample_event_ids.intersection(covered_event_ids)) / len(sample_event_ids) if sample_event_ids else 0.0
        )
        if overlap_ratio >= 0.7:
            continue

        samples = [
            {"title": row["title"] or "", "comment": row["comment"] or "", "domain": row["domain"] or ""}
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
                "event_id": representative["event_id"],
                "event_meta_id": representative["event_meta_id"],
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
        covered_event_ids.update(sample_event_ids)

    return topic_records, publishable_count, tainted_count


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
    query.awaitTermination()


if __name__ == "__main__":
    main()
