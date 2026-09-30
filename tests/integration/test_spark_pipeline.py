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


def _event(event_id, title, comment="", bot=False, minor=False, user="editor", **fields):
    change = {
        "id": event_id,
        "type": "edit",
        "namespace": 0,
        "wiki": "enwiki",
        "server_name": "en.wikipedia.org",
        "title": title,
        "comment": comment,
        "user": user,
        "bot": bot,
        "minor": minor,
        "timestamp": 1790000000 + event_id,
        "title_url": f"https://en.wikipedia.org/wiki/{title.replace(' ', '_')}",
        "length": {"old": 1000, "new": 1000 + 10 * event_id},
        "revision": {"old": 5000 + event_id, "new": 6000 + event_id},
        "meta": {"id": f"meta-{event_id}", "domain": "en.wikipedia.org", "uri": "u", "dt": "d"},
    }
    change.update(fields)
    return json.dumps(change)


def _bronze(spark, events):
    from pyspark.sql.functions import current_timestamp

    bronze = spark.createDataFrame([(event,) for event in events], "raw_json string")
    return bronze.withColumn("kafka_timestamp", current_timestamp()).withColumn("ingestion_ts", current_timestamp())


def test_silver_keeps_human_article_edits_and_classifies_them(spark):
    import silver_processing

    events = [
        _event(1, "Artemis II", "Added crew details"),
        _event(2, "Artemis II", "Automated fix", bot=True),
        _event(3, "Artemis II", "typo", minor=True),
        _event(4, "Category:Spaceflight", "Added page", namespace=14),
        _event(5, "Orion (spacecraft)", "Undid revision 123 by Vandal"),
        _event(6, "12345", "stats"),
        _event(7, "Q42", "wbeditentity-update", wiki="wikidatawiki", server_name="www.wikidata.org"),
        _event(8, "Artemis II", "[[:Artemis II]] added to category", type="categorize"),
        _event(9, "Artemis II", "New article", type="new", length={"new": 2500}, revision={"new": 7000}),
    ]

    # Explicit defaults: the module-level config depends on the environment of the test run
    config = silver_processing.SelectionConfig()
    silver_rows = silver_processing.transform_to_silver(_bronze(spark, events), config).collect()
    rows = {row.event_id: row for row in silver_rows}

    # bots, minor edits, other wikis, categorisation events and non-article namespaces are dropped
    assert set(rows) == {1, 5, 6, 9}
    assert rows[1].moderation_status == "publishable" and rows[1].is_publishable_candidate
    assert rows[5].moderation_status == "revert_signal" and not rows[5].is_publishable_candidate
    assert rows[6].moderation_status == "numeric"
    assert rows[1].editorial_topic_key == "artemis ii"
    assert (rows[1].rev_old, rows[1].rev_new, rows[1].byte_delta) == (5001, 6001, 10)
    assert (rows[9].change_type, rows[9].rev_old, rows[9].byte_delta) == ("new", None, 2500)


def test_silver_filter_is_configurable(spark):
    import silver_processing
    from common.editorial_common import ChangeFilter

    config = silver_processing.SelectionConfig(change_filter=ChangeFilter.from_settings("*", "*", "*"))
    events = [
        _event(1, "Category:Spaceflight", "Added page", namespace=14),
        _event(2, "Q42", "update", wiki="wikidatawiki", server_name="www.wikidata.org"),
    ]

    silver_rows = silver_processing.transform_to_silver(_bronze(spark, events), config).collect()
    rows = {row.event_id: row for row in silver_rows}

    assert set(rows) == {1, 2}
    # the structured namespace, not the title, marks non-article pages
    assert rows[1].moderation_status == "namespace"
    assert rows[2].moderation_status != "namespace"


def test_bursts_need_distinct_editors_and_skip_reverted_articles(spark):
    from datetime import datetime, timedelta, timezone

    import silver_processing

    events = []
    # Breaking news: 5 edits by 4 different people
    for offset, user in enumerate(["ana", "ben", "cai", "dee", "ana"]):
        events.append(_event(10 + offset, "Hurricane Milton", "Landfall update", user=user))
    # One person saving a draft many times: busy, but not news
    for offset in range(8):
        events.append(_event(20 + offset, "Solo Draft Topic", "Expanding", user="solo"))
    # A crowd edit that gets reverted during the hold period
    for offset, user in enumerate(["eve", "fox", "gus", "hal", "eve"]):
        events.append(_event(30 + offset, "Artemis II", "Crew rumour", user=user))
    events.append(_event(40, "Artemis II", "Undid revision 6034 by Eve", user="ivy"))

    config = silver_processing.SelectionConfig(min_edits=5, min_editors=3, lookback_minutes=30, hold_minutes=5)
    silver_df = silver_processing.transform_to_silver(_bronze(spark, events), config).cache()
    now = datetime.fromtimestamp(1790000000, tz=timezone.utc) + timedelta(minutes=10)

    publishable_df, tainted = silver_processing.publishable_events(silver_df, now, config)
    records = silver_processing.build_topic_records(publishable_df, now, config)

    assert tainted == 5
    assert [record["topic_label"] for record in records] == ["Hurricane Milton"]
    milton = records[0]
    assert (milton["topic_event_count"], milton["topic_editor_count"]) == (5, 4)
    assert milton["topic_bytes_added"] == sum(10 * event_id for event_id in range(10, 15))
    samples = json.loads(milton["samples_json"])
    assert {sample["rev_new"] for sample in samples} == {6010, 6011, 6012, 6013, 6014}
    assert all(sample["server_name"] == "en.wikipedia.org" for sample in samples)


def test_offline_selection_tool_replays_a_recorded_sample(spark, tmp_path):
    import offline_topics

    sample = tmp_path / "sample.jsonl"
    lines = [_event(10 + i, "Hurricane Milton", "Landfall", user=f"user{i}") for i in range(5)]
    lines += [_event(20 + i, "Solo Draft Topic", "Expanding", user="solo") for i in range(8)]
    sample.write_text("\n".join(lines) + "\n", encoding="utf-8")
    out = tmp_path / "candidates.csv"

    exit_code = offline_topics.main(
        ["select", "--input", str(sample), "--out", str(out), "--min-editors", "3", "--min-edits", "5"]
    )

    rows = offline_topics.read_rows(str(out))
    assert exit_code == 0
    assert [row["topic_label"] for row in rows] == ["Hurricane Milton"]
    assert rows[0]["peak_editors"] == "5" and rows[0]["newsworthy"] == ""
