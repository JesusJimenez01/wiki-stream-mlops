import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, coalesce, col, length, lit, size

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "wikipedia")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "wikipedia123")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "lakehouse")

GOLD_REQUIRE_DATA = os.getenv("GOLD_REQUIRE_DATA", "false").lower() == "true"
GOLD_MIN_SUCCESS_RATE = float(os.getenv("GOLD_MIN_SUCCESS_RATE", "0.70"))
GOLD_MAX_EMPTY_HEADLINES = int(os.getenv("GOLD_MAX_EMPTY_HEADLINES", "0"))
GOLD_MAX_EMPTY_SUMMARIES = int(os.getenv("GOLD_MAX_EMPTY_SUMMARIES", "0"))
GOLD_SHOW_SAMPLE = os.getenv("GOLD_SHOW_SAMPLE", "false").lower() == "true"


def main() -> None:
    spark = (
        SparkSession.builder.appName("GoldQualityReport")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASSWORD)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    gold = spark.read.format("delta").load(f"s3a://{MINIO_BUCKET}/gold/wiki_news")
    if "topic_label" not in gold.columns:
        gold = gold.withColumn("topic_label", col("topic_term"))
    if "grounded" not in gold.columns:  # stories written before grounding existed
        gold = gold.withColumn("grounded", lit(None).cast("boolean"))
    gold_snapshot = gold.cache()

    total = gold_snapshot.count()
    success = gold_snapshot.filter(col("inference_ok")).count()
    fallback = gold_snapshot.filter(~col("inference_ok")).count()
    # Model answered, but the editorial guardrails had to template the headline/summary
    rejected = gold_snapshot.filter(col("inference_error").startswith("Rejected")).count()
    grounded = gold_snapshot.filter(col("inference_ok") & coalesce(col("grounded"), lit(False))).count()
    empty_headline = gold_snapshot.filter((col("headline").isNull()) | (length(col("headline")) == 0)).count()
    empty_summary = gold_snapshot.filter((col("summary").isNull()) | (length(col("summary")) == 0)).count()
    empty_tags = gold_snapshot.filter(col("tags").isNull() | (size(col("tags")) == 0)).count()
    stats = gold_snapshot.select(
        avg(length(col("headline"))).alias("avg_headline_len"),
        avg(length(col("summary"))).alias("avg_summary_len"),
    ).collect()[0]
    dup = gold_snapshot.groupBy("headline", "topic_term").count().filter(col("count") > 1).count()
    success_rate = (success / total) if total else 0.0

    print("=== GOLD QUALITY CHECK ===")
    for key, value in [
        ("TOTAL_NEWS", total),
        ("SUCCESS_INFERENCE", success),
        ("FALLBACK_INFERENCE", fallback),
        ("SUCCESS_RATE", f"{success_rate * 100.0:.2f}%"),
        ("GUARDRAIL_REJECTED", rejected),
        ("GROUNDED_SUCCESS_RATE", f"{(grounded / success * 100.0) if success else 0.0:.2f}%"),
        ("EMPTY_HEADLINE", empty_headline),
        ("EMPTY_SUMMARY", empty_summary),
        ("EMPTY_TAGS", empty_tags),
        ("AVG_HEADLINE_LEN", f"{float(stats['avg_headline_len'] or 0):.2f}"),
        ("AVG_SUMMARY_LEN", f"{float(stats['avg_summary_len'] or 0):.2f}"),
        ("DUPLICATED_TOPIC_HEADLINES", dup),
    ]:
        print(f"{key}={value}")

    if GOLD_SHOW_SAMPLE:
        print("GOLD_SAMPLE:")
        gold_snapshot.orderBy(col("gold_ts").desc()).select(
            "gold_ts", "topic_term", "topic_label", "topic_event_count", "headline", "summary", "tags", "inference_ok"
        ).show(5, truncate=120)

    checks = []
    if GOLD_REQUIRE_DATA:
        checks.append((total > 0, "No news in Gold"))

    checks.extend(
        [
            (success_rate >= GOLD_MIN_SUCCESS_RATE, f"Success rate below threshold ({GOLD_MIN_SUCCESS_RATE:.2f})"),
            (empty_headline <= GOLD_MAX_EMPTY_HEADLINES, "There are more empty headlines than allowed"),
            (empty_summary <= GOLD_MAX_EMPTY_SUMMARIES, "There are more empty summaries than allowed"),
        ]
    )
    failed = [msg for ok, msg in checks if not ok]
    gold_snapshot.unpersist()
    spark.stop()

    if failed:
        print("GOLD_QUALITY_STATUS=FAIL")
        for msg in failed:
            print(f" - {msg}")
        sys.exit(1)

    print("GOLD_QUALITY_STATUS=PASS")


if __name__ == "__main__":
    main()
