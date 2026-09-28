import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, from_json, lit, lower, trim
from pyspark.sql.types import BooleanType, LongType, StringType, StructField, StructType

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "wikipedia")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "wikipedia123")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "lakehouse")
QUALITY_REQUIRE_DATA = os.getenv("QUALITY_REQUIRE_DATA", "false").lower() == "true"
QUALITY_SHOW_SAMPLE = os.getenv("QUALITY_SHOW_SAMPLE", "false").lower() == "true"


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("WikiQualityCheck")
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


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("ERROR")

    schema = StructType(
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

    bronze = (
        spark.read.format("delta")
        .load(f"s3a://{MINIO_BUCKET}/bronze/wiki_raw")
        .withColumn("event", from_json(col("raw_json"), schema))
    )

    bronze_compact = bronze.select(
        col("event.bot").alias("bot"),
        col("event.minor").alias("minor"),
    )

    bronze_total = bronze_compact.count()
    bronze_bot = bronze_compact.filter(coalesce(col("bot"), lit(False))).count()
    bronze_minor = bronze_compact.filter(coalesce(col("minor"), lit(False))).count()

    silver = spark.read.format("delta").load(f"s3a://{MINIO_BUCKET}/silver/wiki_clean")
    for missing_col, default_expr in [
        ("editorial_signal", lit(None).cast("string")),
        ("editorial_priority", lit(None).cast("int")),
        ("is_editorial_topic_candidate", lit(None).cast("boolean")),
        ("editorial_topic_key", lit(None).cast("string")),
        ("editorial_topic_label", lit(None).cast("string")),
    ]:
        if missing_col not in silver.columns:
            silver = silver.withColumn(missing_col, default_expr)

    silver_total = silver.count()
    silver_bad_bot_minor = silver.filter(
        (coalesce(col("bot"), lit(False))) | (coalesce(col("minor"), lit(False)))
    ).count()

    silver_bad_normalization = silver.filter(
        (col("title_normalized") != lower(trim(col("title"))))
        | (col("comment_normalized") != lower(trim(col("comment"))))
    ).count()

    silver_missing_editorial = silver.filter(
        col("editorial_signal").isNull()
        | col("editorial_priority").isNull()
        | col("is_editorial_topic_candidate").isNull()
    ).count()

    silver_bad_editorial_topic = silver.filter(
        col("is_editorial_topic_candidate")
        & (col("editorial_topic_key").isNull() | col("editorial_topic_label").isNull())
    ).count()

    print("=== WIKI QUALITY CHECK ===")
    for key, value in [
        ("BRONZE_TOTAL", bronze_total),
        ("BRONZE_BOT_TRUE", bronze_bot),
        ("BRONZE_MINOR_TRUE", bronze_minor),
        ("SILVER_TOTAL", silver_total),
        ("SILVER_BAD_BOT_OR_MINOR", silver_bad_bot_minor),
        ("SILVER_BAD_NORMALIZATION", silver_bad_normalization),
        ("SILVER_MISSING_EDITORIAL_SIGNALS", silver_missing_editorial),
        ("SILVER_BAD_EDITORIAL_TOPIC", silver_bad_editorial_topic),
    ]:
        print(f"{key}={value}")

    checks = []

    if QUALITY_REQUIRE_DATA:
        checks.append((bronze_total > 0, "Bronze has no data"))
        checks.append((silver_total > 0, "Silver has no data"))

    checks.extend(
        [
            (silver_bad_bot_minor == 0, "Silver contains bot/minor records"),
            (silver_bad_normalization == 0, "Silver contains incorrect normalization"),
            (silver_missing_editorial == 0, "Silver contains records without editorial signals"),
            (silver_bad_editorial_topic == 0, "Silver contains topic candidates without topic key/label"),
            (silver_total <= bronze_total, "Silver cannot have more rows than Bronze"),
        ]
    )

    failed = [msg for ok, msg in checks if not ok]

    if QUALITY_SHOW_SAMPLE:
        print("SILVER_SAMPLE:")
        silver.select(
            "title",
            "title_normalized",
            "editorial_signal",
            "editorial_priority",
            "is_editorial_topic_candidate",
            "event_ts",
        ).orderBy(col("event_ts").desc()).show(5, truncate=90)

    spark.stop()

    if failed:
        print("QUALITY_STATUS=FAIL")
        for msg in failed:
            print(f" - {msg}")
        sys.exit(1)

    print("QUALITY_STATUS=PASS")


if __name__ == "__main__":
    main()
