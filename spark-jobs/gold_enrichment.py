"""
Gold Enrichment — Wikipedia Pipeline (Phase 4)

Turns curated Silver topics into news stories with a local LLM (Ollama),
then applies editorial guardrails, deduplication and story lifecycle rules.
"""

import json
import logging
import os
import re
import uuid
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher
from typing import Any, Dict, List, Optional, Tuple
from urllib.request import Request, urlopen

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, desc, lit
from pyspark.sql.types import ArrayType, BooleanType, DoubleType, LongType, StringType, StructField, StructType

from common.editorial_common import contains_foreign_script, resolve_display_topic_label, resolve_source_label
from common.spark_common import MINIO_BUCKET, create_spark_session, wait_for_delta_source
from common.wiki_context import build_topic_context, is_grounded, render_context

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("wiki-gold")

OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://ollama:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen3:8b")
OLLAMA_TIMEOUT_SECONDS = int(os.getenv("OLLAMA_TIMEOUT_SECONDS", "60"))

SILVER_TOPICS_PATH = f"s3a://{MINIO_BUCKET}/silver/wiki_topics"
GOLD_OUTPUT_PATH = f"s3a://{MINIO_BUCKET}/gold/wiki_news"
GOLD_CHECKPOINT_PATH = f"s3a://{MINIO_BUCKET}/gold/_checkpoint"
GOLD_METRICS_PATH = f"s3a://{MINIO_BUCKET}/gold/_metrics"

TRIGGER_INTERVAL = os.getenv("GOLD_TRIGGER_INTERVAL", "30 seconds")
GOLD_STARTING_VERSION = os.getenv("GOLD_STARTING_VERSION", "latest")
GOLD_MAX_EVENTS_PER_BATCH = int(os.getenv("GOLD_MAX_EVENTS_PER_BATCH", "20"))
GOLD_DEDUP_LOOKBACK_HOURS = int(os.getenv("GOLD_DEDUP_LOOKBACK_HOURS", "6"))
GOLD_DEDUP_MAX_CANDIDATES = int(os.getenv("GOLD_DEDUP_MAX_CANDIDATES", "400"))
GOLD_HEADLINE_SIM_THRESHOLD = float(os.getenv("GOLD_HEADLINE_SIM_THRESHOLD", "0.92"))
GOLD_SUMMARY_SIM_THRESHOLD = float(os.getenv("GOLD_SUMMARY_SIM_THRESHOLD", "0.90"))
GOLD_TOKEN_JACCARD_THRESHOLD = float(os.getenv("GOLD_TOKEN_JACCARD_THRESHOLD", "0.82"))
GOLD_MIN_UPDATE_INTERVAL_MINUTES = int(os.getenv("GOLD_MIN_UPDATE_INTERVAL_MINUTES", "45"))
GOLD_MIN_TOPIC_COUNT_DELTA = int(os.getenv("GOLD_MIN_TOPIC_COUNT_DELTA", "5"))
GOLD_STORY_STALE_HOURS = int(os.getenv("GOLD_STORY_STALE_HOURS", "12"))
GOLD_WIKI_CONTEXT_ENABLED = os.getenv("GOLD_WIKI_CONTEXT_ENABLED", "true").strip().lower() in {"1", "true", "yes"}
GOLD_CONTEXT_MAX_DIFFS = int(os.getenv("GOLD_CONTEXT_MAX_DIFFS", "3"))
GOLD_CONTEXT_MAX_CHARS = int(os.getenv("GOLD_CONTEXT_MAX_CHARS", "1500"))
GOLD_TOKEN_MIN_LEN = int(os.getenv("GOLD_TOKEN_MIN_LEN", "4"))
STOPWORDS = {
    word.strip().lower()
    for word in os.getenv(
        "GOLD_TOPIC_STOPWORDS",
        "wikipedia,wikidata,wikimedia,commons,article,file,edit,updated,update,category,page,using,added,user,minor,bot,with,from,that,this,para,como,donde,sobre,con,from,the,and,for,are,was,you,your,http,https,www,wiki,batch,short,removed,quickstatements,wbeditentity,toollabs,property,create",
    ).split(",")
    if word.strip()
}

ABSTRACT_HEADLINE_PREFIXES = (
    "update",
    "various",
    "trend",
    "changes detected",
    "changes registered",
    "changes in",
    "new changes",
)

COLLECTIVE_SUMMARY_PHRASES = (
    "various articles",
    "changes detected",
    "changes in the category",
)

ROBOTIC_SUMMARY_PHRASES = (
    "concentrates the most relevant change of this block",
    "the update reinforces its informative interest",
)

NAMESPACE_PREFIXES = (
    "category:",
    "categoria:",
    "categoría:",
    "template:",
    "user:",
    "wikipedia:",
    "help:",
    "file:",
    "talk:",
    "portal:",
    "draft:",
)

SYSTEM_PROMPT = (
    "You are the Editor in Chief of the Breaking News section of a digital newspaper. "
    "Your job is to transform recent Wikipedia changes into short, clear, and engaging news stories.\n\n"
    "EDITORIAL RULES:\n"
    "0. Language: always draft and return the final content exclusively in natural English. Translate any sample that "
    "arrives in another language within this response.\n"
    "1. Tone: direct, urgent, objective, and impactful. Use active voice. Avoid robotic, academic, or bureaucratic "
    "language.\n"
    "2. Headline: maximum 10 words. Must capture immediate attention and highlight the real novelty.\n"
    "3. Summary: maximum 3 short sentences. Apply the inverted pyramid: what happened and why it matters.\n"
    "4. Topic_label: return a short label of 2 to 5 words, in natural English, to show as a section or kicker.\n"
    "5. Tags: return an array of 2 to 4 concrete and useful categories, also in English.\n"
    "6. Quality: avoid generic headlines like 'Wikipedia Update' or empty summaries. Prioritize novelty, impact, "
    "conflict, change, or public relevance.\n"
    "7. Angle: always choose the dominant journalistic angle of the case, for example, breakthrough, crisis, dispute, "
    "trend, discovery, market, politics, science, or culture.\n"
    "8. Framing: do not make the source platform the subject of the news unless the change is literally about it. "
    "Avoid repeating Wikipedia, Wikidata, or Wikimedia if the reader understands the piece without that crutch.\n"
    "9. Rigor: do not invent facts, figures, causes, or consequences that are not supported by the received context. "
    "If the context is limited, write conservatively but interestingly.\n"
    "10. Output: do not leave words in non-Latin alphabets in topic_label, headline, summary, or tags.\n\n"
    "SYSTEM RULE:\n"
    "You must respond solely and exclusively with a valid JSON object with the keys topic_label, headline, summary, "
    "and tags. "
    "Do not add text before or after. Do not use markdown."
)

# Sent as Ollama's ``format`` so the model is constrained to this shape (structured outputs)
NEWS_JSON_SCHEMA = {
    "type": "object",
    "properties": {
        "topic_label": {"type": "string"},
        "headline": {"type": "string"},
        "summary": {"type": "string"},
        "tags": {"type": "array", "items": {"type": "string"}},
    },
    "required": ["topic_label", "headline", "summary", "tags"],
}

FACT_RULES = (
    "FACT RULES:\n"
    "- Base every factual claim on the article context and the added text above; edit comments are only hints.\n"
    "- If they do not say what happened, do not guess an event: report that the article is drawing a surge of edits "
    "and explain who or what the subject is using the article context.\n"
)

GOLD_SCHEMA = StructType(
    [
        StructField("topic_term", StringType(), True),
        StructField("topic_label", StringType(), True),
        StructField("topic_event_count", LongType(), True),
        StructField("topic_editor_count", LongType(), True),
        StructField("event_id", LongType(), True),
        StructField("event_meta_id", StringType(), True),
        StructField("domain", StringType(), True),
        StructField("article_uri", StringType(), True),
        StructField("title", StringType(), True),
        StructField("comment", StringType(), True),
        StructField("editor_user", StringType(), True),
        StructField("title_url", StringType(), True),
        StructField("event_ts", StringType(), True),
        StructField("silver_ts", StringType(), True),
        StructField("headline", StringType(), True),
        StructField("summary", StringType(), True),
        StructField("tags", ArrayType(StringType()), True),
        StructField("inference_ok", BooleanType(), False),
        StructField("inference_error", StringType(), True),
        StructField("grounded", BooleanType(), True),
        StructField("dedup_score", DoubleType(), True),
        StructField("is_update", BooleanType(), False),
        StructField("duplicate_of_gold_ts", StringType(), True),
        StructField("story_id", StringType(), False),
        StructField("published_at", StringType(), False),
        StructField("updated_at", StringType(), False),
        StructField("update_seq", LongType(), False),
        StructField("is_live_event", BooleanType(), False),
        StructField("gold_ts", StringType(), False),
        StructField("source_raw_json", StringType(), True),
    ]
)


def maybe_disable_thinking(prompt: str) -> str:
    return f"/no_think\n{prompt}" if OLLAMA_MODEL.lower().startswith("qwen3") else prompt


def truncate_words(text: str, max_words: int) -> str:
    words = [word for word in str(text or "").split() if word]
    if len(words) <= max_words:
        return " ".join(words)
    return " ".join(words[:max_words])


def normalize_brief_text(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "").strip().lower())


def starts_with_abstract_prefix(text: str) -> bool:
    normalized = normalize_brief_text(text)
    return any(normalized.startswith(prefix) for prefix in ABSTRACT_HEADLINE_PREFIXES)


def looks_collective_summary(text: str) -> bool:
    normalized = normalize_brief_text(text)
    return any(phrase in normalized for phrase in COLLECTIVE_SUMMARY_PHRASES)


def pick_story_anchor(topic_term: str, samples: List[Dict[str, str]]) -> str:
    candidates = [str(sample.get("title", "")).strip() for sample in samples] + [str(topic_term or "").strip()]
    for candidate in candidates:
        if not candidate:
            continue
        if contains_foreign_script(candidate):
            continue
        normalized = normalize_brief_text(candidate)
        if any(normalized.startswith(prefix) for prefix in NAMESPACE_PREFIXES):
            continue
        if len(candidate) < 3:
            continue
        return candidate
    return "a key protagonist"


def enforce_human_framing(
    topic_term: str,
    samples: List[Dict[str, str]],
    topic_label: str,
    headline: str,
    summary: str,
    tags: List[str],
) -> Tuple[str, str, str, List[str], List[str]]:
    """
    Apply the editorial guardrails. Returns the cleaned fields plus the list of
    issues that forced a templated headline or summary (empty when the model's
    own text survived), so callers can refuse to publish templated stories.
    """
    issues: List[str] = []
    anchor = pick_story_anchor(topic_term, samples)
    if contains_foreign_script(anchor):
        anchor = "a key protagonist"
    clean_topic_label = str(topic_label or "").strip() or truncate_words(anchor, 5)
    if contains_foreign_script(clean_topic_label):
        clean_topic_label = "News"
    clean_headline = str(headline or "").strip()
    clean_summary = str(summary or "").strip()

    headline_norm = normalize_brief_text(clean_headline)
    misleading_agency = bool(
        re.search(
            r"\b(updates|updated|renews|renewed)\b.*\b(biography|entry|profile|article)\b",
            headline_norm,
        )
    )

    if not clean_headline or starts_with_abstract_prefix(clean_headline) or misleading_agency:
        issues.append(
            "headline: empty"
            if not clean_headline
            else "headline: platform edit as the subject"
            if misleading_agency
            else "headline: abstract opening"
        )
        clean_headline = truncate_words(f"{anchor} returns to focus after new updates", 10)

    if contains_foreign_script(clean_headline):
        issues.append("headline: non-Latin script")
        clean_headline = "New relevant breaking update"

    summary_norm = normalize_brief_text(clean_summary)
    looks_robotic_summary = any(phrase in summary_norm for phrase in ROBOTIC_SUMMARY_PHRASES)

    if (
        not clean_summary
        or looks_collective_summary(clean_summary)
        or looks_robotic_summary
        or contains_foreign_script(clean_summary)
    ):
        issues.append(
            "summary: empty"
            if not clean_summary
            else "summary: non-Latin script"
            if contains_foreign_script(clean_summary)
            else "summary: collective or robotic phrasing"
        )
        clean_summary = (
            f"Recent changes linked to {anchor} have been registered. "
            "The update gains informative relevance at this time."
        )

    clean_tags = [str(tag).strip() for tag in (tags or []) if str(tag).strip()]
    return clean_topic_label, clean_headline, clean_summary, (clean_tags or ["news"]), issues


def _sample_line(sample: Dict[str, Any]) -> str:
    line = (
        f"- title: {sample.get('title', '')} | comment: {sample.get('comment', '')} "
        f"| domain: {sample.get('domain', '')}"
    )
    if sample.get("byte_delta") is not None:
        line += f" | size change: {int(sample['byte_delta']):+d} bytes"
    return line


def build_topic_prompt(
    topic_term: str,
    topic_event_count: int,
    samples: List[Dict[str, Any]],
    context: Optional[Dict[str, Any]] = None,
    editor_count: Optional[int] = None,
) -> str:
    sample_lines = [_sample_line(sample) for sample in samples]
    context_block = "\n".join(sample_lines) if sample_lines else "- no samples"
    source_label = resolve_source_label(samples)
    editors_line = f"Distinct editors: {int(editor_count)}\n" if editor_count else ""
    facts = render_context(context)
    facts_block = f"{facts}{FACT_RULES}\n" if facts else ""
    prompt = (
        f"{SYSTEM_PROMPT}\n\n"
        "You receive a topic already curated by the analytical layer. Write a human news story focused on a specific "
        "protagonist.\n"
        f"Predominant source project: {source_label}\n"
        f"Recurring topic: {topic_term}\n"
        f"Related events: {topic_event_count}\n"
        f"{editors_line}"
        "Sample changes:\n"
        f"{context_block}\n\n"
        f"{facts_block}"
        "The headline must start with the main protagonist (person, team, mission, institution, work, or specific "
        "place).\n"
        "Forbidden to open with collective or technical approaches like 'Update', 'Several articles', 'Trend', or "
        "'Changes detected'.\n"
        "Do not attribute the edit to the protagonist with phrases like 'X updates their biography'; present the fact "
        "as editorial changes to their public coverage.\n"
        "If technical noise or text in non-Latin alphabets appears in samples/categories, ignore it and do not copy it "
        "into the output.\n"
        "Do not open the headline with 'Wikipedia', 'Wikidata', or 'Wikimedia' unless the news event is the platform "
        "itself.\n"
        "Return exclusively the JSON."
    )
    return maybe_disable_thinking(prompt)


def extract_json_object(text: str) -> Dict[str, Any]:
    """Parse the model output, tolerating prose around the JSON object."""
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if not match:
            raise ValueError("No valid JSON found in the model's response") from None
        payload = json.loads(match.group(0))
    if not isinstance(payload, dict):
        raise ValueError("The model's response is not a JSON object")
    return payload


def normalize_model_output(payload: Dict[str, Any]) -> Tuple[str, str, str, List[str]]:
    topic_label = str(payload.get("topic_label", "")).strip()
    headline = str(payload.get("headline", "")).strip()
    summary = str(payload.get("summary", "")).strip()
    tags_raw = payload.get("tags", [])
    if isinstance(tags_raw, list):
        tags = [str(tag).strip() for tag in tags_raw if str(tag).strip()]
    elif isinstance(tags_raw, str):
        tags = [part.strip() for part in tags_raw.split(",") if part.strip()]
    else:
        tags = []
    return topic_label, headline, summary, tags or ["news"]


def call_ollama(
    prompt: str, topic_term: str, samples: List[Dict[str, str]]
) -> Tuple[str, str, str, List[str], bool, Optional[str]]:
    request = Request(
        f"{OLLAMA_BASE_URL.rstrip('/')}/api/generate",
        data=json.dumps({"model": OLLAMA_MODEL, "prompt": prompt, "stream": False, "format": NEWS_JSON_SCHEMA}).encode(
            "utf-8"
        ),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(request, timeout=OLLAMA_TIMEOUT_SECONDS) as response:
            response_obj = json.loads(response.read().decode("utf-8"))
        if not isinstance(response_obj, dict):
            raise ValueError(f"Unexpected Ollama response body: {type(response_obj).__name__}")
        topic_label, headline, summary, tags = normalize_model_output(
            extract_json_object(str(response_obj.get("response", "")).strip())
        )
        topic_label, headline, summary, tags, issues = enforce_human_framing(
            topic_term, samples, topic_label, headline, summary, tags
        )
        if issues:
            # A templated headline or summary is not the model's story: keep it out of the front page
            return topic_label, headline, summary, tags, False, "Rejected by editorial guardrails: " + "; ".join(issues)
        combined = " ".join([topic_label, headline, summary, " ".join(tags)]).strip()
        if contains_foreign_script(combined):
            raise ValueError("The model's response was not completely in English")
        return topic_label, headline, summary, tags, True, None
    # OSError covers URLError, timeouts and dropped connections; ValueError covers bad JSON.
    # Any of them yields a fallback story instead of failing the whole micro-batch.
    except (OSError, ValueError) as exc:
        topic_label, headline, summary, tags, _ = enforce_human_framing(
            topic_term,
            samples,
            "",
            "",
            "Could not generate an automatic summary for this event.",
            ["fallback"],
        )
        return topic_label, headline, summary, tags, False, str(exc)


def write_metrics(
    spark: SparkSession,
    batch_id: int,
    total: int,
    generated: int = 0,
    success: int = 0,
    dedup_discarded: int = 0,
    updates: int = 0,
) -> None:
    failed = generated - success
    metrics_schema = StructType(
        [
            StructField("batch_id", LongType(), False),
            StructField("total_events", LongType(), False),
            StructField("generated_events", LongType(), False),
            StructField("success_events", LongType(), False),
            StructField("failed_events", LongType(), False),
            StructField("failed_pct", DoubleType(), False),
            StructField("dedup_discarded_events", LongType(), False),
            StructField("update_events", LongType(), False),
            StructField("processed_at", StringType(), False),
        ]
    )
    row = (
        int(batch_id),
        int(total),
        int(generated),
        int(success),
        int(failed),
        (failed / generated * 100.0) if generated else 0.0,
        int(dedup_discarded),
        int(updates),
        datetime.now(timezone.utc).isoformat(),
    )
    spark.createDataFrame([row], schema=metrics_schema).write.format("delta").mode("append").option(
        "mergeSchema", "true"
    ).save(GOLD_METRICS_PATH)


def normalize_similarity_text(text: str) -> str:
    normalized = re.sub(r"[^a-zA-Z0-9áéíóúüñçàèìòùâêîôûãõäëïöÿ\s]", " ", (text or "").lower())
    return re.sub(r"\s+", " ", normalized).strip()


def build_similarity_tokens(text: str) -> set:
    return {
        token
        for token in normalize_similarity_text(text).split(" ")
        if token and len(token) >= GOLD_TOKEN_MIN_LEN and token not in STOPWORDS
    }


def jaccard_similarity(left: set, right: set) -> float:
    if not left and not right:
        return 1.0
    if not left or not right:
        return 0.0
    return len(left.intersection(right)) / len(left.union(right))


def parse_iso_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def should_allow_update(
    now_utc: datetime,
    previous_gold_ts: Optional[str],
    previous_topic_event_count: Optional[int],
    current_topic_event_count: int,
) -> bool:
    previous_dt = parse_iso_datetime(previous_gold_ts)
    if previous_dt is None:
        return False
    minutes_since_previous = (now_utc - previous_dt).total_seconds() / 60.0
    topic_delta = abs(int(current_topic_event_count) - int(previous_topic_event_count or 0))
    return minutes_since_previous >= GOLD_MIN_UPDATE_INTERVAL_MINUTES and topic_delta >= GOLD_MIN_TOPIC_COUNT_DELTA


def load_recent_topic_candidates(spark: SparkSession, topic_terms: List[str]) -> Dict[str, List[Dict[str, Any]]]:
    if not topic_terms:
        return {}
    cutoff_iso = (datetime.now(timezone.utc) - timedelta(hours=GOLD_DEDUP_LOOKBACK_HOURS)).isoformat()
    try:
        gold_df = spark.read.format("delta").load(GOLD_OUTPUT_PATH)
        for missing_col, default_expr in [
            ("story_id", lit(None).cast("string")),
            ("published_at", lit(None).cast("string")),
            ("update_seq", lit(0).cast("long")),
        ]:
            if missing_col not in gold_df.columns:
                gold_df = gold_df.withColumn(missing_col, default_expr)
        candidates_df = (
            gold_df.filter(col("topic_term").isin(topic_terms))
            .filter(col("gold_ts") >= cutoff_iso)
            .select(
                "topic_term",
                "topic_event_count",
                "headline",
                "summary",
                "gold_ts",
                "story_id",
                "published_at",
                "update_seq",
            )
            .orderBy(desc("gold_ts"))
            .limit(GOLD_DEDUP_MAX_CANDIDATES)
        )
    except Exception as exc:
        logger.info("No Gold history yet for deduplication (initial batch): %s", exc)
        return {term: [] for term in topic_terms}

    grouped: Dict[str, List[Dict[str, Any]]] = {term: [] for term in topic_terms}
    for row in candidates_df.collect():
        grouped.setdefault(row["topic_term"], []).append(
            {
                "topic_event_count": int(row["topic_event_count"] or 0),
                "headline": row["headline"] or "",
                "summary": row["summary"] or "",
                "gold_ts": row["gold_ts"],
                "story_id": row["story_id"],
                "published_at": row["published_at"],
                "update_seq": int(row["update_seq"] or 0),
            }
        )
    return grouped


def find_recent_publication(topic_candidates: List[Dict[str, Any]], now_utc: datetime) -> Optional[str]:
    """
    Return the gold_ts of a story on this topic published less than
    GOLD_MIN_UPDATE_INTERVAL_MINUTES ago, if any.

    Such a topic can never produce a new story or an update yet, so this check
    runs before the LLM call to avoid spending GPU time on discarded drafts.
    """
    for candidate in topic_candidates:
        candidate_dt = parse_iso_datetime(candidate.get("gold_ts"))
        if candidate_dt and (now_utc - candidate_dt).total_seconds() / 60 < GOLD_MIN_UPDATE_INTERVAL_MINUTES:
            return candidate.get("gold_ts")
    return None


def evaluate_duplicate_news(
    topic_event_count: int, headline: str, summary: str, topic_candidates: List[Dict[str, Any]], now_utc: datetime
) -> Tuple[bool, float, Optional[str], bool]:
    if not topic_candidates:
        return False, 0.0, None, False
    recent_gold_ts = find_recent_publication(topic_candidates, now_utc)
    if recent_gold_ts is not None:
        return True, 1.0, recent_gold_ts, False

    current_headline = normalize_similarity_text(headline)
    current_summary = normalize_similarity_text(summary)
    current_tokens = build_similarity_tokens(f"{current_headline} {current_summary}")
    best_score = 0.0
    best_match_ts: Optional[str] = None
    should_mark_as_update = False

    for candidate in topic_candidates:
        candidate_headline = normalize_similarity_text(candidate.get("headline", ""))
        candidate_summary = normalize_similarity_text(candidate.get("summary", ""))
        candidate_tokens = build_similarity_tokens(f"{candidate_headline} {candidate_summary}")
        headline_sim = SequenceMatcher(None, current_headline, candidate_headline).ratio()
        summary_sim = SequenceMatcher(None, current_summary, candidate_summary).ratio()
        token_sim = jaccard_similarity(current_tokens, candidate_tokens)
        score = max(headline_sim, summary_sim, token_sim)
        if score > best_score:
            best_score = score
            best_match_ts = candidate.get("gold_ts")
        if (
            headline_sim < GOLD_HEADLINE_SIM_THRESHOLD
            and summary_sim < GOLD_SUMMARY_SIM_THRESHOLD
            and token_sim < GOLD_TOKEN_JACCARD_THRESHOLD
        ):
            continue
        if should_allow_update(
            now_utc, candidate.get("gold_ts"), candidate.get("topic_event_count"), topic_event_count
        ):
            should_mark_as_update = True
            continue
        return True, score, candidate.get("gold_ts"), False
    return False, best_score, best_match_ts, should_mark_as_update


def resolve_story_info(topic_candidates: List[Dict[str, Any]], is_update: bool, now_utc: datetime) -> Dict[str, Any]:
    if is_update and topic_candidates:
        for candidate in sorted(topic_candidates, key=lambda item: item.get("gold_ts", ""), reverse=True):
            if candidate.get("story_id"):
                return {
                    "story_id": candidate["story_id"],
                    "published_at": candidate.get("published_at") or candidate.get("gold_ts", now_utc.isoformat()),
                    "updated_at": now_utc.isoformat(),
                    "update_seq": int(candidate.get("update_seq") or 0) + 1,
                    "is_live_event": True,
                }
    return {
        "story_id": str(uuid.uuid4()),
        "published_at": now_utc.isoformat(),
        "updated_at": now_utc.isoformat(),
        "update_seq": 1,
        "is_live_event": False,
    }


def conclude_stale_stories(spark: SparkSession) -> int:
    cutoff_iso = (datetime.now(timezone.utc) - timedelta(hours=GOLD_STORY_STALE_HOURS)).isoformat()
    try:
        gold_df = spark.read.format("delta").load(GOLD_OUTPUT_PATH)
    except Exception:
        return 0
    if "is_live_event" not in gold_df.columns or "updated_at" not in gold_df.columns:
        return 0
    try:
        target_count = gold_df.filter(col("is_live_event") & (col("updated_at") < cutoff_iso)).count()
        if target_count == 0:
            return 0
        spark.sql(
            f"UPDATE delta.`{GOLD_OUTPUT_PATH}` SET is_live_event = false "
            f"WHERE is_live_event = true AND updated_at < '{cutoff_iso}'"
        )
        logger.info("Concluded %s stale live stories (cutoff=%s)", target_count, cutoff_iso)
        return target_count
    except Exception as exc:
        logger.warning("Error concluding stale stories: %s", exc)
        return 0


def main() -> None:
    spark = create_spark_session("WikiGoldEnrichment")
    spark.sparkContext.setLogLevel("WARN")

    logger.info("Starting Gold enrichment from %s", SILVER_TOPICS_PATH)
    wait_for_delta_source(spark, SILVER_TOPICS_PATH, "Silver topics")
    topics_stream_df = (
        spark.readStream.format("delta").option("startingVersion", GOLD_STARTING_VERSION).load(SILVER_TOPICS_PATH)
    )

    def process_batch(batch_df: DataFrame, batch_id: int) -> None:
        incoming_topics = batch_df.count()
        if incoming_topics == 0:
            logger.info("batch=%s total=0 (no new topics)", batch_id)
            return

        concluded = conclude_stale_stories(spark)
        if concluded:
            logger.info("batch=%s concluded_stale=%s", batch_id, concluded)

        now_utc = datetime.now(timezone.utc)
        topic_rows = (
            batch_df.orderBy(desc("silver_topic_ts"), desc("topic_event_count"))
            .limit(GOLD_MAX_EVENTS_PER_BATCH)
            .collect()
        )
        recent_candidates_by_topic = load_recent_topic_candidates(spark, [row["topic_term"] for row in topic_rows])

        topic_news_records: List[Dict[str, Any]] = []
        dedup_discarded = 0
        update_events = 0
        for row in topic_rows:
            topic_candidates = recent_candidates_by_topic.get(row["topic_term"], [])
            if find_recent_publication(topic_candidates, now_utc) is not None:
                # Silver re-emits hot topics every micro-batch; skip them before paying for inference
                dedup_discarded += 1
                continue

            samples = json.loads(row["samples_json"] or "[]")
            editor_count = row.asDict().get("topic_editor_count")
            context = (
                build_topic_context(samples, GOLD_CONTEXT_MAX_DIFFS, GOLD_CONTEXT_MAX_CHARS)
                if GOLD_WIKI_CONTEXT_ENABLED
                else None
            )
            translated_topic_label, headline, summary, tags, inference_ok, inference_error = call_ollama(
                build_topic_prompt(
                    row["topic_term"],
                    int(row["topic_event_count"] or 0),
                    samples,
                    context=context,
                    editor_count=editor_count,
                ),
                row["topic_term"],
                samples,
            )

            is_duplicate, dedup_score, duplicate_of_gold_ts, is_update = evaluate_duplicate_news(
                topic_event_count=int(row["topic_event_count"] or 0),
                headline=headline,
                summary=summary,
                topic_candidates=topic_candidates,
                now_utc=now_utc,
            )
            if is_duplicate:
                dedup_discarded += 1
                continue
            if is_update:
                update_events += 1

            story_info = resolve_story_info(topic_candidates, is_update, now_utc)
            topic_label = resolve_display_topic_label(
                translated_topic_label,
                row["topic_term"],
                tags,
                row["domain"],
                headline,
            )
            topic_news_records.append(
                {
                    "topic_term": row["topic_term"],
                    "topic_label": topic_label,
                    "topic_event_count": int(row["topic_event_count"] or 0),
                    "topic_editor_count": int(editor_count) if editor_count is not None else None,
                    "event_id": row["event_id"],
                    "event_meta_id": row["event_meta_id"],
                    "domain": row["domain"],
                    "article_uri": row["article_uri"],
                    "title": row["title"],
                    "comment": row["comment"],
                    "editor_user": row["editor_user"],
                    "title_url": row["title_url"],
                    "event_ts": row["event_ts"],
                    "silver_ts": row["silver_ts"],
                    "headline": headline,
                    "summary": summary,
                    "tags": tags,
                    "inference_ok": inference_ok,
                    "inference_error": inference_error,
                    "grounded": is_grounded(context),
                    "dedup_score": float(dedup_score),
                    "is_update": bool(is_update),
                    "duplicate_of_gold_ts": duplicate_of_gold_ts,
                    "story_id": story_info["story_id"],
                    "published_at": story_info["published_at"],
                    "updated_at": story_info["updated_at"],
                    "update_seq": int(story_info["update_seq"]),
                    "is_live_event": story_info["is_live_event"],
                    "gold_ts": now_utc.isoformat(),
                    "source_raw_json": row["source_raw_json"],
                }
            )
            topic_candidates.append(
                {
                    "topic_event_count": int(row["topic_event_count"] or 0),
                    "headline": headline,
                    "summary": summary,
                    "gold_ts": now_utc.isoformat(),
                    "story_id": story_info["story_id"],
                    "published_at": story_info["published_at"],
                    "update_seq": int(story_info["update_seq"]),
                }
            )
            recent_candidates_by_topic[row["topic_term"]] = topic_candidates

        if not topic_news_records:
            logger.info("batch=%s incoming_topics=%s generated=0 dedup=%s", batch_id, incoming_topics, dedup_discarded)
            write_metrics(spark, batch_id, incoming_topics, dedup_discarded=dedup_discarded, updates=update_events)
            return

        enriched_df = spark.createDataFrame(topic_news_records, schema=GOLD_SCHEMA)
        enriched_df.write.format("delta").mode("append").option("mergeSchema", "true").save(GOLD_OUTPUT_PATH)
        success_events = sum(1 for record in topic_news_records if record["inference_ok"])
        failed_events = len(topic_news_records) - success_events
        write_metrics(
            spark,
            batch_id,
            incoming_topics,
            generated=len(topic_news_records),
            success=success_events,
            dedup_discarded=dedup_discarded,
            updates=update_events,
        )
        logger.info(
            "batch=%s incoming_topics=%s gen=%s dedup=%s upd=%s ok=%s fail=%s",
            batch_id,
            incoming_topics,
            len(topic_news_records),
            dedup_discarded,
            update_events,
            success_events,
            failed_events,
        )

    query = (
        topics_stream_df.writeStream.foreachBatch(process_batch)
        .option("checkpointLocation", GOLD_CHECKPOINT_PATH)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    logger.info(
        "Gold enrichment started → output=%s source=%s model=%s trigger=%s",
        GOLD_OUTPUT_PATH,
        SILVER_TOPICS_PATH,
        OLLAMA_MODEL,
        TRIGGER_INTERVAL,
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
