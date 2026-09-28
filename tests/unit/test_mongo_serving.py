"""MongoDB serving: document mapping, idempotent upserts and batch chunking."""

from datetime import datetime, timezone
from unittest.mock import MagicMock

from pyspark.sql import Row

import mongo_serving as serving


def _gold_row(story_id, update_seq=1, **extra):
    base = {
        "story_id": story_id,
        "update_seq": update_seq,
        "headline": f"Headline {story_id}",
        "summary": "Summary",
        "tags": ["space"],
        "is_live_event": True,
        "inference_ok": True,
        "gold_ts": "2026-09-28T12:00:00+00:00",
        "published_at": "2026-09-28T11:00:00Z",
        "topic_event_count": 7,
        "dedup_score": 0.12,
    }
    base.update(extra)
    return base


def test_document_id_is_story_and_sequence():
    doc = serving.to_document(_gold_row("abc", update_seq=3))

    assert doc["_id"] == "abc:3"
    assert doc["update_seq"] == 3
    assert doc["timestamp"] == datetime(2026, 9, 28, 12, 0, tzinfo=timezone.utc)
    assert doc["published_at"] == datetime(2026, 9, 28, 11, 0, tzinfo=timezone.utc)
    assert doc["tags"] == ["space"]


def test_chunked_splits_without_losing_rows():
    rows = list(range(7))
    assert serving.chunked(rows, 3) == [[0, 1, 2], [3, 4, 5], [6]]
    assert serving.chunked(rows, 0) == [[r] for r in rows]


def test_large_batches_are_written_in_chunks_not_truncated(monkeypatch):
    written = []
    monkeypatch.setattr(serving, "SERVING_BATCH_LIMIT", 2)
    monkeypatch.setattr(serving, "write_documents_to_mongo", lambda rows, source, batch_id: written.append(rows))

    batch_df = MagicMock()
    batch_df.orderBy.return_value.collect.return_value = [Row(**_gold_row(f"s{i}")) for i in range(5)]

    serving.write_batch_to_mongo(batch_df, batch_id=1)

    assert [len(chunk) for chunk in written] == [2, 2, 1]
    assert [row["story_id"] for chunk in written for row in chunk] == ["s0", "s1", "s2", "s3", "s4"]


def test_upserts_are_idempotent_and_indexes_created_once(monkeypatch):
    client = MagicMock()
    collection = client.__enter__.return_value.__getitem__.return_value.__getitem__.return_value
    monkeypatch.setattr(serving, "MongoClient", MagicMock(return_value=client))
    monkeypatch.setattr(serving, "INDEXES_CREATED", False)

    serving.write_documents_to_mongo([_gold_row("a"), _gold_row("b", update_seq=2)], "stream", 1)
    serving.write_documents_to_mongo([_gold_row("c")], "stream", 2)

    operations = collection.bulk_write.call_args_list[0].args[0]
    assert [op._filter for op in operations] == [{"_id": "a:1"}, {"_id": "b:2"}]
    assert all(op._upsert for op in operations)
    assert collection.create_index.call_count == 5  # only on the first write
