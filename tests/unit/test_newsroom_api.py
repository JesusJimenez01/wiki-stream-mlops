"""Newsroom API: endpoints, ranking and Prometheus label hygiene (MongoDB mocked)."""

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient
from prometheus_client import generate_latest

import app as newsroom


def _doc(story_id, topic, headline, **extra):
    doc = {
        "_id": f"{story_id}:1",
        "story_id": story_id,
        "update_seq": 1,
        "topic_term": topic,
        "topic_label": topic.title(),
        "headline": headline,
        "summary": f"Summary of {headline}",
        "tags": ["news"],
        "timestamp": datetime(2026, 9, 28, 12, 0, tzinfo=timezone.utc),
        "inference_ok": True,
        "topic_event_count": 5,
        "domain": "en.wikipedia.org",
    }
    doc.update(extra)
    return doc


@pytest.fixture
def collection(monkeypatch):
    mock = MagicMock()
    monkeypatch.setattr(newsroom, "_get_collection", lambda: mock)
    return mock


@pytest.fixture
def client():
    return TestClient(newsroom.app)


def test_health_ok(client, monkeypatch):
    monkeypatch.setattr(newsroom, "_mongo_client", MagicMock())
    assert client.get("/health").json() == {"status": "ok"}


def test_health_reports_mongo_outage(client, monkeypatch):
    broken = MagicMock()
    broken.return_value.admin.command.side_effect = RuntimeError("connection refused")
    monkeypatch.setattr(newsroom, "_mongo_client", broken)

    response = client.get("/health")

    assert response.status_code == 503
    assert "connection refused" in response.json()["detail"]


def test_news_feed_hides_foreign_script_and_limits_topics_in_the_head(client, collection):
    docs = [_doc(f"moon-{i}", "moon", f"Moon story number {i}") for i in range(4)]
    docs += [_doc("mars-1", "mars", "Mars rover finds new rock layers"), _doc("ru-1", "misc", "Новости дня")]
    collection.aggregate.return_value = docs

    items = client.get("/api/news?limit=3").json()["items"]

    assert [item["story_id"] for item in items].count("ru-1") == 0
    assert sum(item["topic_term"] == "moon" for item in items) <= 2
    assert any(item["topic_term"] == "mars" for item in items)
    assert all(item["inference_ok"] for item in items)


def test_news_feed_filters_by_topic(client, collection):
    collection.aggregate.return_value = []

    client.get("/api/news?topic=moon")

    match_stage = collection.aggregate.call_args.args[0][0]["$match"]
    assert match_stage == {"inference_ok": True, "topic_term": "moon"}


def test_story_not_found(client, collection):
    collection.find_one.return_value = None
    assert client.get("/api/news/missing").status_code == 404


def test_story_returns_latest_update(client, collection):
    collection.find_one.return_value = _doc("abc", "moon", "Crew named", update_seq=3)

    story = client.get("/api/news/abc").json()

    assert story["update_seq"] == 3
    assert collection.find_one.call_args.kwargs["sort"] == [("update_seq", -1)]


def test_http_metrics_use_route_templates_not_raw_ids(client, collection):
    collection.find_one.return_value = None
    for story_id in ("story-aaa", "story-bbb", "story-ccc"):
        client.get(f"/api/news/{story_id}")
    client.get("/definitely-not-a-route")

    exposition = generate_latest().decode()

    assert 'path="/api/news/{story_id}"' in exposition
    assert 'path="unmatched"' in exposition
    assert "story-aaa" not in exposition and "definitely-not-a-route" not in exposition


def test_index_page_is_served_regardless_of_working_directory(client, monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    response = client.get("/")
    assert response.status_code == 200
    assert "app.js" in response.text
