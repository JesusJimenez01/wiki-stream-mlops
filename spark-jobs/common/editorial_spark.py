from pyspark.sql import DataFrame
from pyspark.sql.functions import coalesce, col, concat_ws, length, lit, lower, trim, when

from common.editorial_common import EDITORIAL_NOISE_REGEX, FOREIGN_SCRIPT_REGEX, LOW_SIGNAL_TOPIC_REGEX, TITLE_NAMESPACE_REGEX


MIN_EDITORIAL_TITLE_LEN = 4


def with_editorial_signals(df: DataFrame) -> DataFrame:
    result_df = df
    if "title_normalized" not in result_df.columns:
        result_df = result_df.withColumn("title_normalized", lower(trim(coalesce(col("title"), lit("")))))
    if "comment_normalized" not in result_df.columns:
        result_df = result_df.withColumn("comment_normalized", lower(trim(coalesce(col("comment"), lit("")))))

    title_normalized = coalesce(col("title_normalized"), lit(""))
    comment_normalized = coalesce(col("comment_normalized"), lit(""))
    analysis_text = lower(concat_ws(" ", title_normalized, comment_normalized))
    title_has_foreign_script = title_normalized.rlike(FOREIGN_SCRIPT_REGEX)
    comment_has_foreign_script = comment_normalized.rlike(FOREIGN_SCRIPT_REGEX)
    title_is_namespace = title_normalized.rlike(TITLE_NAMESPACE_REGEX) | (
        title_has_foreign_script & title_normalized.contains(":")
    )
    title_is_numeric = title_normalized.rlike(r"^[0-9]+$")
    has_editorial_noise = analysis_text.rlike(EDITORIAL_NOISE_REGEX)
    is_low_signal_topic = title_normalized.rlike(LOW_SIGNAL_TOPIC_REGEX)
    is_editorial_topic_candidate = (
        (length(title_normalized) >= MIN_EDITORIAL_TITLE_LEN)
        & (~title_is_namespace)
        & (~title_is_numeric)
        & (~is_low_signal_topic)
        & (~has_editorial_noise)
    )

    return (
        result_df.withColumn("title_has_foreign_script", title_has_foreign_script)
        .withColumn("comment_has_foreign_script", comment_has_foreign_script)
        .withColumn("title_is_namespace", title_is_namespace)
        .withColumn("title_is_numeric", title_is_numeric)
        .withColumn("has_editorial_noise", has_editorial_noise)
        .withColumn("is_low_signal_topic", is_low_signal_topic)
        .withColumn("is_editorial_topic_candidate", is_editorial_topic_candidate)
        .withColumn(
            "editorial_priority",
            when(col("is_editorial_topic_candidate"), lit(3))
            .when(col("title_is_namespace") | col("title_is_numeric"), lit(-3))
            .when(col("is_low_signal_topic"), lit(-3))
            .when(col("has_editorial_noise"), lit(-2))
            .when(col("title_has_foreign_script"), lit(1))
            .when(length(title_normalized) >= MIN_EDITORIAL_TITLE_LEN, lit(1))
            .otherwise(lit(0)),
        )
        .withColumn(
            "editorial_signal",
            when(col("is_editorial_topic_candidate"), lit("topic_candidate"))
            .when(col("title_is_namespace"), lit("namespace"))
            .when(col("title_is_numeric"), lit("numeric"))
            .when(col("is_low_signal_topic"), lit("low_signal_topic"))
            .when(col("has_editorial_noise"), lit("technical_noise"))
            .when(col("title_has_foreign_script"), lit("foreign_script"))
            .otherwise(lit("weak_context")),
        )
        .withColumn(
            "editorial_topic_key",
            when(col("is_editorial_topic_candidate"), title_normalized).otherwise(lit(None).cast("string")),
        )
        .withColumn(
            "editorial_topic_label",
            when(col("is_editorial_topic_candidate"), col("title")).otherwise(lit(None).cast("string")),
        )
    )