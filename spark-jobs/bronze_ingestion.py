"""
Bronze Ingestion — Wikipedia Pipeline (Phase 2)
Spark Structured Streaming: Redpanda (wiki-raw) → MinIO (Delta Lake)
"""

import logging
import os

from pyspark.sql.functions import col, current_timestamp

from common.spark_common import MINIO_BUCKET, create_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("wiki-bronze")

REDPANDA_BROKER = os.getenv("REDPANDA_BROKER", "redpanda:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_RAW", "wiki-raw")
CHECKPOINT_PATH = f"s3a://{MINIO_BUCKET}/bronze/_checkpoint"
OUTPUT_PATH = f"s3a://{MINIO_BUCKET}/bronze/wiki_raw"
TRIGGER_INTERVAL = os.getenv("BRONZE_TRIGGER_INTERVAL", "30 seconds")


def main() -> None:
    spark = create_spark_session("WikiBronzeIngestion")
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Connecting to Redpanda (%s) topic '%s'", REDPANDA_BROKER, KAFKA_TOPIC)

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", REDPANDA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    bronze_df = raw_stream.select(
        col("key").cast("string").alias("key"),
        col("value").cast("string").alias("raw_json"),
        col("topic"), col("partition"), col("offset"),
        col("timestamp").alias("kafka_timestamp"),
    ).withColumn("ingestion_ts", current_timestamp())

    query = (
        bronze_df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start(OUTPUT_PATH)
    )

    logger.info("Bronze ingestion started → %s (trigger every %s)", OUTPUT_PATH, TRIGGER_INTERVAL)
    query.awaitTermination()


if __name__ == "__main__":
    main()
