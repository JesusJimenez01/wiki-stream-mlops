"""
Newsroom API — Wikipedia Pipeline (Phase 6)

Serves the generated news (web + JSON API) from MongoDB and exposes
Prometheus metrics about the product, the lakehouse and the HTTP layer.
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List
from urllib.parse import urlparse

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, Response
from fastapi.staticfiles import StaticFiles
from minio import Minio
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)
from pymongo import DESCENDING, MongoClient

APP_DIR = Path(__file__).resolve().parent
STATIC_DIR = APP_DIR / "static"

# In the container `common/` is copied next to app.py; in the repo it lives in spark-jobs/
SHARED_HELPERS_DIR = APP_DIR.parent / "spark-jobs"
if SHARED_HELPERS_DIR.exists():
    sys.path.insert(0, str(SHARED_HELPERS_DIR))

from common.editorial_common import contains_foreign_script  # noqa: E402

MONGO_HOST = os.getenv("MONGO_HOST", "mongodb")
MONGO_PORT = int(os.getenv("MONGO_PORT", "27017"))
MONGO_USER = os.getenv("MONGO_INITDB_ROOT_USERNAME", "wikipedia")
MONGO_PASSWORD = os.getenv("MONGO_INITDB_ROOT_PASSWORD", "wikipedia123")
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "wikipedia")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "news")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "wikipedia")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "wikipedia123")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "lakehouse")
NEWSROOM_METRICS_TTL_SECONDS = int(os.getenv("NEWSROOM_METRICS_TTL_SECONDS", "60"))

APP_TITLE = "Wikipedia Newsroom"

ABSTRACT_HEADLINE_PREFIXES = (
    "update",
    "various",
    "trend",
    "changes detected",
    "changes registered",
    "changes in",
)

logger = logging.getLogger("newsroom-api")

REQUEST_COUNTER = Counter(
    "newsroom_http_requests_total",
    "Total HTTP requests",
    ["method", "path", "status"],
)
REQUEST_LATENCY = Histogram(
    "newsroom_http_request_duration_seconds",
    "HTTP request duration in seconds",
    ["method", "path"],
    buckets=(0.01, 0.05, 0.1, 0.2, 0.5, 1, 2, 5),
)

NEWS_TOTAL = Gauge("wiki_news_total", "Total news in MongoDB")
LIVE_NEWS_TOTAL = Gauge("wiki_live_news_total", "Total live stories in MongoDB")
INFERENCE_SUCCESS_RATIO = Gauge("wiki_inference_success_ratio", "Ratio of inference_ok=true")
LATEST_NEWS_AGE_MINUTES = Gauge("wiki_latest_news_age_minutes", "Minutes since latest published news")
RAW_EVENTS_TOTAL = Gauge("wiki_raw_events_total", "Total raw events currently stored in Bronze")
BRONZE_STORAGE_BYTES = Gauge("wiki_bronze_storage_bytes", "Storage used by Bronze prefix in bytes")
SILVER_STORAGE_BYTES = Gauge("wiki_silver_storage_bytes", "Storage used by Silver prefix in bytes")
GOLD_STORAGE_BYTES = Gauge("wiki_gold_storage_bytes", "Storage used by Gold prefix in bytes")
LAKEHOUSE_STORAGE_BYTES = Gauge("wiki_lakehouse_storage_bytes", "Storage used by the full lakehouse bucket in bytes")
MONGODB_STORAGE_BYTES = Gauge("wiki_mongodb_storage_bytes", "MongoDB storage used by the newsroom database in bytes")
PROJECT_STORAGE_BYTES = Gauge("wiki_project_storage_bytes", "Combined lakehouse and MongoDB storage in bytes")

_STORAGE_CACHE: Dict[str, Any] = {"expires_at": 0.0, "value": None}


app = FastAPI(title=APP_TITLE)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


def _mongo_uri() -> str:
    return f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/?authSource=admin"


@lru_cache(maxsize=1)
def _mongo_client() -> MongoClient:
    """Process-wide MongoDB client: it is thread-safe and keeps its own connection pool."""
    return MongoClient(_mongo_uri(), serverSelectionTimeoutMS=5000)


def _get_collection():
    return _mongo_client()[MONGO_DATABASE][MONGO_COLLECTION]


def _get_minio_client() -> Minio:
    parsed = urlparse(MINIO_ENDPOINT)
    endpoint = parsed.netloc or parsed.path
    secure = parsed.scheme == "https"
    return Minio(endpoint, access_key=MINIO_USER, secret_key=MINIO_PASSWORD, secure=secure)


def _parse_delta_stats(stats_raw: Any) -> int:
    if not stats_raw:
        return 0
    try:
        stats = json.loads(stats_raw)
    except (TypeError, json.JSONDecodeError):
        return 0
    return int(stats.get("numRecords", 0) or 0)


def _sum_prefix_size(client: Minio, prefix: str) -> int:
    return sum(int(obj.size or 0) for obj in client.list_objects(MINIO_BUCKET, prefix=prefix, recursive=True))


def _compute_bronze_snapshot(client: Minio) -> Dict[str, int]:
    active_files: Dict[str, Dict[str, int]] = {}
    log_objects = sorted(
        [
            obj
            for obj in client.list_objects(MINIO_BUCKET, prefix="bronze/wiki_raw/_delta_log/", recursive=True)
            if obj.object_name.endswith(".json")
        ],
        key=lambda obj: obj.object_name,
    )

    for obj in log_objects:
        response = client.get_object(MINIO_BUCKET, obj.object_name)
        try:
            payload = response.read().decode("utf-8")
        finally:
            response.close()
            response.release_conn()

        for line in payload.splitlines():
            if not line.strip():
                continue
            try:
                action = json.loads(line)
            except json.JSONDecodeError:
                continue

            add_action = action.get("add")
            if add_action:
                path = str(add_action.get("path") or "")
                if not path:
                    continue
                active_files[path] = {
                    "size": int(add_action.get("size", 0) or 0),
                    "records": _parse_delta_stats(add_action.get("stats")),
                }
                continue

            remove_action = action.get("remove")
            if remove_action:
                path = str(remove_action.get("path") or "")
                if path:
                    active_files.pop(path, None)

    return {
        "raw_events_total": sum(item["records"] for item in active_files.values()),
        "bronze_data_bytes": sum(item["size"] for item in active_files.values()),
    }


def _storage_snapshot() -> Dict[str, int]:
    now = time.time()
    cached = _STORAGE_CACHE.get("value")
    if cached and now < float(_STORAGE_CACHE.get("expires_at", 0.0)):
        return cached

    snapshot = {
        "raw_events_total": 0,
        "bronze_storage_bytes": 0,
        "silver_storage_bytes": 0,
        "gold_storage_bytes": 0,
        "lakehouse_storage_bytes": 0,
    }

    try:
        client = _get_minio_client()
        snapshot["bronze_storage_bytes"] = _sum_prefix_size(client, "bronze/")
        snapshot["silver_storage_bytes"] = _sum_prefix_size(client, "silver/")
        snapshot["gold_storage_bytes"] = _sum_prefix_size(client, "gold/")
        snapshot["lakehouse_storage_bytes"] = _sum_prefix_size(client, "")
        snapshot.update(_compute_bronze_snapshot(client))
    except Exception as exc:
        logger.warning("MinIO metrics unavailable: %s", exc)

    _STORAGE_CACHE["value"] = snapshot
    _STORAGE_CACHE["expires_at"] = now + NEWSROOM_METRICS_TTL_SECONDS
    return snapshot


def _safe_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if value is None:
        return datetime.now(timezone.utc)
    try:
        text = str(value).replace("Z", "+00:00")
        parsed = datetime.fromisoformat(text)
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except ValueError:
        return datetime.now(timezone.utc)


def _topic_distribution(collection) -> List[Dict[str, Any]]:
    pipeline = [
        {
            "$group": {
                "_id": {"topic_term": "$topic_term", "topic_label": "$topic_label", "domain": "$domain"},
                "count": {"$sum": 1},
            }
        },
        {"$sort": {"count": -1}},
        {"$limit": 8},
    ]
    rows = list(collection.aggregate(pipeline))
    return [
        {
            "topic": (row.get("_id") or {}).get("topic_label") or (row.get("_id") or {}).get("topic_term") or "unknown",
            "topic_value": (row.get("_id") or {}).get("topic_term") or "unknown",
            "count": int(row.get("count", 0)),
        }
        for row in rows
    ]


def _metrics_snapshot() -> Dict[str, Any]:
    collection = _get_collection()
    total = int(collection.count_documents({}))
    live_total = int(collection.count_documents({"is_live_event": True}))
    success_total = int(collection.count_documents({"inference_ok": True}))
    ratio = (float(success_total) / float(total)) if total else 0.0
    db_stats = collection.database.command("dbStats")
    mongo_storage_bytes = int(db_stats.get("storageSize", 0) or 0)
    storage_snapshot = _storage_snapshot()
    project_storage_bytes = int(storage_snapshot["lakehouse_storage_bytes"]) + mongo_storage_bytes

    latest = collection.find_one({}, sort=[("timestamp", DESCENDING)])
    if latest:
        latest_ts = _safe_datetime(latest.get("timestamp"))
        age_minutes = max((datetime.now(timezone.utc) - latest_ts).total_seconds() / 60.0, 0.0)
    else:
        age_minutes = 0.0

    NEWS_TOTAL.set(total)
    LIVE_NEWS_TOTAL.set(live_total)
    INFERENCE_SUCCESS_RATIO.set(ratio)
    LATEST_NEWS_AGE_MINUTES.set(age_minutes)
    RAW_EVENTS_TOTAL.set(int(storage_snapshot["raw_events_total"]))
    BRONZE_STORAGE_BYTES.set(int(storage_snapshot["bronze_storage_bytes"]))
    SILVER_STORAGE_BYTES.set(int(storage_snapshot["silver_storage_bytes"]))
    GOLD_STORAGE_BYTES.set(int(storage_snapshot["gold_storage_bytes"]))
    LAKEHOUSE_STORAGE_BYTES.set(int(storage_snapshot["lakehouse_storage_bytes"]))
    MONGODB_STORAGE_BYTES.set(mongo_storage_bytes)
    PROJECT_STORAGE_BYTES.set(project_storage_bytes)

    return {
        "total_news": total,
        "live_news": live_total,
        "inference_success_ratio": ratio,
        "latest_news_age_minutes": age_minutes,
        "raw_events_total": int(storage_snapshot["raw_events_total"]),
        "bronze_storage_bytes": int(storage_snapshot["bronze_storage_bytes"]),
        "silver_storage_bytes": int(storage_snapshot["silver_storage_bytes"]),
        "gold_storage_bytes": int(storage_snapshot["gold_storage_bytes"]),
        "lakehouse_storage_bytes": int(storage_snapshot["lakehouse_storage_bytes"]),
        "mongodb_storage_bytes": mongo_storage_bytes,
        "project_storage_bytes": project_storage_bytes,
        "top_topics": _topic_distribution(collection),
    }


def _route_label(request) -> str:
    """
    Metric label for the request path.

    Uses the route template (``/api/news/{story_id}``) instead of the raw URL:
    labelling by raw path would create one time series per story id or scanned
    URL, an unbounded cardinality that grows Prometheus memory without limit.
    """
    route = request.scope.get("route")
    return getattr(route, "path", None) or "unmatched"


@app.middleware("http")
async def metrics_middleware(request, call_next):
    start = time.perf_counter()
    response = await call_next(request)
    elapsed = time.perf_counter() - start
    path = _route_label(request)
    method = request.method
    status = str(response.status_code)
    REQUEST_COUNTER.labels(method=method, path=path, status=status).inc()
    REQUEST_LATENCY.labels(method=method, path=path).observe(elapsed)
    return response


@app.get("/", response_class=HTMLResponse)
def root() -> HTMLResponse:
    return HTMLResponse((STATIC_DIR / "index.html").read_text(encoding="utf-8"))


def _serialize_doc(doc: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": str(doc.get("_id")),
        "story_id": doc.get("story_id"),
        "update_seq": int(doc.get("update_seq") or 1),
        "is_live_event": bool(doc.get("is_live_event", False)),
        "is_update": bool(doc.get("is_update", False)),
        "inference_ok": bool(doc.get("inference_ok", False)),
        "topic_term": doc.get("topic_term"),
        "topic_label": doc.get("topic_label") or doc.get("topic_term") or "General",
        "topic_event_count": int(doc.get("topic_event_count") or 0),
        "headline": doc.get("headline") or "No headline",
        "summary": doc.get("summary") or "",
        "tags": doc.get("tags") or [],
        "timestamp": _safe_datetime(doc.get("timestamp")).isoformat(),
        "title_url": doc.get("title_url"),
        "domain": doc.get("domain") or "wikipedia",
    }


def _normalize_text(text: Any) -> str:
    return " ".join(str(text or "").strip().lower().split())


def _is_abstract_headline(text: Any) -> bool:
    normalized = _normalize_text(text)
    return any(normalized.startswith(prefix) for prefix in ABSTRACT_HEADLINE_PREFIXES)


def _interesting_score(item: Dict[str, Any]) -> float:
    score = 0.0
    if item.get("is_live_event"):
        score += 30.0
    if item.get("is_update"):
        score += 8.0
    if item.get("inference_ok"):
        score += 5.0

    score += min(float(item.get("topic_event_count") or 0), 25.0)
    score += min(float(item.get("update_seq") or 1), 6.0)

    headline = str(item.get("headline") or "")
    if _is_abstract_headline(headline):
        score -= 15.0
    if 20 <= len(headline) <= 90:
        score += 3.0

    if not item.get("summary"):
        score -= 8.0
    if contains_foreign_script(headline) or contains_foreign_script(item.get("summary") or ""):
        score -= 20.0

    return score


def _rank_and_diversify(items: List[Dict[str, Any]], limit: int) -> List[Dict[str, Any]]:
    ranked = sorted(
        items,
        key=lambda item: (_interesting_score(item), item.get("timestamp", "")),
        reverse=True,
    )

    selected: List[Dict[str, Any]] = []
    per_topic: Dict[str, int] = {}
    head_window = min(limit, 12)
    max_per_topic_in_head = 2

    for item in ranked:
        topic_key = str(item.get("topic_term") or item.get("topic_label") or "general").lower()
        if len(selected) < head_window and per_topic.get(topic_key, 0) >= max_per_topic_in_head:
            continue
        selected.append(item)
        per_topic[topic_key] = per_topic.get(topic_key, 0) + 1
        if len(selected) >= limit:
            break

    if len(selected) < limit:
        selected_ids = {item.get("id") for item in selected}
        for item in ranked:
            if item.get("id") in selected_ids:
                continue
            selected.append(item)
            if len(selected) >= limit:
                break

    return selected


def _has_foreign_content(doc: Dict[str, Any]) -> bool:
    return contains_foreign_script(doc.get("headline") or "") or contains_foreign_script(doc.get("summary") or "")


@app.get("/health")
def health() -> Dict[str, str]:
    try:
        _mongo_client().admin.command("ping")
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"MongoDB unavailable: {exc}") from exc
    return {"status": "ok"}


@app.get("/api/news")
def get_news(
    limit: int = Query(default=30, ge=1, le=100),
    topic: str | None = Query(default=None),
) -> Dict[str, Any]:
    collection = _get_collection()
    match_stage: Dict[str, Any] = {"inference_ok": True}
    if topic:
        match_stage["topic_term"] = topic

    candidate_limit = min(limit * 4, 300)
    pipeline = [
        {"$match": match_stage},
        {"$sort": {"update_seq": -1}},
        {"$group": {"_id": "$story_id", "doc": {"$first": "$$ROOT"}}},
        {"$replaceRoot": {"newRoot": "$doc"}},
        {"$sort": {"timestamp": -1}},
        {"$limit": candidate_limit},
    ]
    rows = [_serialize_doc(doc) for doc in collection.aggregate(pipeline) if not _has_foreign_content(doc)]
    rows = _rank_and_diversify(rows, limit)
    return {"items": rows, "count": len(rows)}


@app.get("/api/news/{story_id}")
def get_story(story_id: str) -> Dict[str, Any]:
    doc = _get_collection().find_one({"story_id": story_id}, sort=[("update_seq", DESCENDING)])
    if not doc:
        raise HTTPException(status_code=404, detail="Story not found")
    return _serialize_doc(doc)


@app.get("/api/stats")
def get_stats() -> Dict[str, Any]:
    snapshot = _metrics_snapshot()
    return snapshot


@app.get("/metrics")
def metrics() -> Response:
    _metrics_snapshot()
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
