"""
Spark + Delta Lake integration tests (local session, no MinIO/Kafka needed).

They exercise the real pipeline code: Silver's transformation and the Gold stream
reader used by the MongoDB serving job. Requires Java 17+; the Delta Lake JARs
are resolved by delta-spark on first run.
"""

import json
import shutil

import pytest

pytest.importorskip("delta")
if shutil.which("java") is None:
    pytest.skip("Java is required for the Spark integration tests", allow_module_level=True)

from delta import configure_spark_with_delta_pip  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def spark():
    builder = (
        SparkSession.builder.master("local[1]")
        .appName("wiki-stream-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


# ---------------------------------------------------------------------------
# Serving: Gold stream survives the UPDATE that closes stale live stories
# ---------------------------------------------------------------------------


def _drain(stream_df, checkpoint, seen):
    def sink(batch_df, _batch_id):
        seen.extend((row.story_id, row.is_live_event) for row in batch_df.collect())

    query = stream_df.writeStream.foreachBatch(sink).option("checkpointLocation", checkpoint)
    query.trigger(availableNow=True).start().awaitTermination()


def test_gold_stream_survives_stale_story_update_and_reemits_new_state(spark, tmp_path):
    import mongo_serving

    gold = str(tmp_path / "gold")
    checkpoint = str(tmp_path / "checkpoint")
    spark.createDataFrame(
        [("s1", 1, True), ("s2", 1, True)], "story_id string, update_seq int, is_live_event boolean"
    ).write.format("delta").save(gold)

    seen = []
    _drain(mongo_serving.read_gold_stream(spark, gold, starting_version="0"), checkpoint, seen)
    assert sorted(seen) == [("s1", True), ("s2", True)]

    # Same statement Gold runs in conclude_stale_stories()
    spark.sql(f"UPDATE delta.`{gold}` SET is_live_event = false WHERE story_id = 's1'")

    seen.clear()
    _drain(mongo_serving.read_gold_stream(spark, gold, starting_version="0"), checkpoint, seen)

    # The rewritten row is re-emitted so MongoDB learns the story is no longer live
    assert ("s1", False) in seen


def test_serving_writes_a_real_micro_batch_in_ordered_chunks(spark, monkeypatch):
    import mongo_serving

    written = []
    monkeypatch.setattr(mongo_serving, "SERVING_BATCH_LIMIT", 2)
    monkeypatch.setattr(mongo_serving, "write_documents_to_mongo", lambda rows, source, batch_id: written.append(rows))
    batch = spark.createDataFrame(
        [(f"s{i}", 1, f"2026-09-28T12:0{i}:00+00:00") for i in (3, 0, 4, 1, 2)],
        "story_id string, update_seq int, gold_ts string",
    ).repartition(3)

    mongo_serving.write_batch_to_mongo(batch, batch_id=7)

    assert [len(chunk) for chunk in written] == [2, 2, 1]
    assert [row["story_id"] for chunk in written for row in chunk] == ["s0", "s1", "s2", "s3", "s4"]


def test_plain_delta_stream_fails_on_update(spark, tmp_path):
    """Documents why the serving reader needs ignoreChanges (regression guard)."""
    gold = str(tmp_path / "gold")
    checkpoint = str(tmp_path / "checkpoint")
    spark.createDataFrame([("s1", True)], "story_id string, is_live_event boolean").write.format("delta").save(gold)

    _drain(spark.readStream.format("delta").load(gold), checkpoint, [])
    spark.sql(f"UPDATE delta.`{gold}` SET is_live_event = false")

    with pytest.raises(Exception, match="DELTA_SOURCE_TABLE_IGNORE_CHANGES"):
        _drain(spark.readStream.format("delta").load(gold), checkpoint, [])


# ---------------------------------------------------------------------------
# Silver: curation rules on real Wikimedia-shaped events
# ---------------------------------------------------------------------------


def _event(event_id, title, comment="", bot=False, minor=False):
    return json.dumps(
        {
            "id": event_id,
            "title": title,
            "comment": comment,
            "user": "editor",
            "bot": bot,
            "minor": minor,
            "timestamp": 1790000000 + event_id,
            "title_url": f"https://en.wikipedia.org/wiki/{title.replace(' ', '_')}",
            "meta": {"id": f"meta-{event_id}", "domain": "en.wikipedia.org", "uri": "u", "dt": "d"},
        }
    )


def test_silver_filters_bots_and_minor_edits_and_classifies_the_rest(spark):
    from pyspark.sql.functions import current_timestamp

    import silver_processing

    raw = [
        (_event(1, "Artemis II", "Added crew details"),),
        (_event(2, "Artemis II", "Automated fix", bot=True),),
        (_event(3, "Artemis II", "typo", minor=True),),
        (_event(4, "Category:Spaceflight", "Added page"),),
        (_event(5, "Orion (spacecraft)", "Undid revision 123 by Vandal"),),
        (_event(6, "12345", "stats"),),
    ]
    bronze = spark.createDataFrame(raw, "raw_json string").withColumn("kafka_timestamp", current_timestamp())
    bronze = bronze.withColumn("ingestion_ts", current_timestamp())

    rows = {row.event_id: row for row in silver_processing.transform_to_silver(bronze).collect()}

    assert set(rows) == {1, 4, 5, 6}  # bot and minor edits are dropped
    assert rows[1].moderation_status == "publishable" and rows[1].is_publishable_candidate
    assert rows[4].moderation_status == "namespace" and not rows[4].is_publishable_candidate
    assert rows[5].moderation_status == "revert_signal" and not rows[5].is_publishable_candidate
    assert rows[6].moderation_status == "numeric"
    assert rows[1].editorial_topic_key == "artemis ii"
