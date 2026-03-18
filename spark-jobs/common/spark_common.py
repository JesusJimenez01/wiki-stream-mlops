"""Shared Spark utilities for the Wikipedia pipeline."""

import logging
import os
import time

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "wikipedia")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "wikipedia123")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "lakehouse")


def create_spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASSWORD)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )


def wait_for_delta_source(spark: SparkSession, path: str, name: str) -> None:
    while True:
        try:
            spark.read.format("delta").load(path).limit(1).count()
            return
        except Exception as exc:
            msg = str(exc)
            if not any(t in msg for t in ["DELTA_SCHEMA_NOT_SET", "Path does not exist", "is not a Delta table"]):
                raise
            logger.info("Esperando %s en %s", name, path)
            time.sleep(5)