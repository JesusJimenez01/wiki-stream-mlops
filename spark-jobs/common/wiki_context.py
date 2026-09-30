"""
Grounding for the LLM: what the article says and what the latest edits added.

Edit summaries alone ("Updated", "ce", "+1") rarely say what happened, so the
model used to guess. This module fetches the facts from two public Wikimedia
REST endpoints (no API key needed):

* ``/api/rest_v1/page/summary/{title}`` — lead extract of the article.
* ``/w/rest.php/v1/revision/{from}/compare/{to}`` — structured diff of an edit.

Every call is best-effort: a slow or failing API only shrinks the prompt back
to titles and edit summaries, it never blocks the Gold micro-batch.
"""

import http.client
import json
import logging
import os
import re
from typing import Any, Callable, Dict, Iterable, List, Optional
from urllib.parse import quote
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)

WIKI_API_TIMEOUT_SECONDS = float(os.getenv("WIKI_API_TIMEOUT_SECONDS", "5"))
WIKI_USER_AGENT = os.getenv(
    "WIKI_USER_AGENT", "wiki-stream-mlops/1.0 (https://github.com/JesusJimenez01/wiki-stream-mlops)"
)

# Line types of the REST compare endpoint, and highlight types inside a changed line
DIFF_ADDED = 1
DIFF_CHANGED = 3
HIGHLIGHT_ADDED = 0

# Only Wikimedia projects: the host comes from event data and must not become an open proxy
WIKIMEDIA_HOST_REGEX = re.compile(
    r"^[a-z0-9-]+(\.[a-z0-9-]+)*\."
    r"(wikipedia|wikimedia|wikidata|wikinews|wikiquote|wikisource|wikivoyage|wiktionary|wikibooks|wikiversity|mediawiki)"
    r"\.org$"
)

FETCH_ERRORS = (OSError, ValueError, http.client.HTTPException)

FetchJson = Callable[[str], Any]


def fetch_json(url: str) -> Any:
    request = Request(url, headers={"User-Agent": WIKI_USER_AGENT, "Accept": "application/json"})
    with urlopen(request, timeout=WIKI_API_TIMEOUT_SECONDS) as response:
        return json.loads(response.read().decode("utf-8"))


def wiki_host(server_name: Any) -> str:
    host = str(server_name or "").strip().lower()
    if not WIKIMEDIA_HOST_REGEX.match(host):
        raise ValueError(f"Not a Wikimedia host: {server_name!r}")
    return host


# ---------------------------------------------------------------------------
# Wikitext → plain text (good enough for a prompt, not a full parser)
# ---------------------------------------------------------------------------

_COMMENT = re.compile(r"<!--.*?-->", re.DOTALL)
_REF = re.compile(r"<ref[^>]*/>|<ref[^>]*>.*?</ref>", re.DOTALL | re.IGNORECASE)
_TEMPLATE = re.compile(r"\{\{[^{}]*\}\}")
_MEDIA_LINK = re.compile(r"\[\[(?:file|image|category):[^\[\]]*\]\]", re.IGNORECASE)
_LINK = re.compile(r"\[\[(?:[^|\[\]]*\|)?([^\[\]]*)\]\]")
_EXTERNAL_LINK = re.compile(r"\[https?://\S+\s*([^\]]*)\]")
_HTML_TAG = re.compile(r"<[^>]+>")
_LEFTOVER_MARKUP = re.compile(r"\{\{|\}\}|\[\[|\]\]|'{2,}|={2,}|^[*#:;|!]+", re.MULTILINE)


def clean_wikitext(text: Any) -> str:
    cleaned = _REF.sub(" ", _COMMENT.sub(" ", str(text or "")))
    for _ in range(3):  # nested templates, innermost first
        cleaned, replaced = _TEMPLATE.subn(" ", cleaned)
        if not replaced:
            break
    cleaned = _MEDIA_LINK.sub(" ", cleaned)
    cleaned = _LINK.sub(r"\1", cleaned)
    cleaned = _EXTERNAL_LINK.sub(r"\1", cleaned)
    cleaned = _HTML_TAG.sub(" ", cleaned)
    cleaned = _LEFTOVER_MARKUP.sub(" ", cleaned)
    return re.sub(r"\s+", " ", cleaned).strip()


# ---------------------------------------------------------------------------
# REST endpoints
# ---------------------------------------------------------------------------


def page_summary(server_name: Any, title: str, fetch: FetchJson = fetch_json) -> str:
    """Lead extract of the article (plain text), or "" when there is none."""
    encoded_title = quote(str(title or "").strip().replace(" ", "_"), safe="")
    if not encoded_title:
        return ""
    payload = fetch(f"https://{wiki_host(server_name)}/api/rest_v1/page/summary/{encoded_title}")
    if not isinstance(payload, dict):
        return ""
    return str(payload.get("extract") or "").strip()


def _highlighted_ranges(text: str, ranges: Iterable[Any], highlight_type: int) -> List[str]:
    # Offsets and lengths are expressed in UTF-8 bytes, not characters
    raw = text.encode("utf-8")
    pieces = []
    for highlight in ranges:
        if not isinstance(highlight, dict) or highlight.get("type") != highlight_type:
            continue
        start, length = int(highlight.get("start", 0)), int(highlight.get("length", 0))
        pieces.append(raw[start : start + length].decode("utf-8", errors="ignore"))
    return pieces


def added_text_from_diff(payload: Any) -> str:
    """Text added by an edit, from the payload of the REST compare endpoint."""
    diff = payload.get("diff") if isinstance(payload, dict) else None
    pieces: List[str] = []
    for line in diff or []:
        if not isinstance(line, dict):
            continue
        text = str(line.get("text") or "")
        if line.get("type") == DIFF_ADDED:
            pieces.append(text)
        elif line.get("type") == DIFF_CHANGED:
            pieces.extend(_highlighted_ranges(text, line.get("highlightRanges") or [], HIGHLIGHT_ADDED))
    return clean_wikitext("\n".join(pieces))


def added_text(server_name: Any, rev_old: Any, rev_new: Any, fetch: FetchJson = fetch_json) -> str:
    url = f"https://{wiki_host(server_name)}/w/rest.php/v1/revision/{int(rev_old)}/compare/{int(rev_new)}"
    return added_text_from_diff(fetch(url))


# ---------------------------------------------------------------------------
# Topic context for the prompt
# ---------------------------------------------------------------------------


def build_topic_context(
    samples: List[Dict[str, Any]],
    max_diffs: int = 3,
    max_chars: int = 1500,
    fetch: FetchJson = fetch_json,
) -> Dict[str, Any]:
    """
    Article lead plus the text added by the largest recent edits of the topic.

    Returns ``{"article": str, "added": [str, ...]}``; missing pieces stay empty.
    """
    context: Dict[str, Any] = {"article": "", "added": []}
    if not samples:
        return context

    lead = samples[0]
    try:
        context["article"] = page_summary(lead.get("server_name") or lead.get("domain"), lead.get("title", ""), fetch)[
            :max_chars
        ]
    except FETCH_ERRORS as exc:
        logger.info("Article summary unavailable for %r: %s", lead.get("title"), exc)

    with_revisions = [sample for sample in samples if sample.get("rev_old") and sample.get("rev_new")]
    largest_edits = sorted(with_revisions, key=lambda sample: int(sample.get("byte_delta") or 0), reverse=True)
    budget = max_chars
    for sample in largest_edits[:max_diffs]:
        if budget <= 0:
            break
        try:
            server_name = sample.get("server_name") or sample.get("domain")
            text = added_text(server_name, sample["rev_old"], sample["rev_new"], fetch)
        except FETCH_ERRORS as exc:
            logger.info("Diff unavailable for revision %s: %s", sample.get("rev_new"), exc)
            continue
        if text and text not in context["added"]:
            context["added"].append(text[:budget])
            budget -= len(context["added"][-1])
    return context


def is_grounded(context: Optional[Dict[str, Any]]) -> bool:
    return bool(context and (context.get("article") or context.get("added")))


def render_context(context: Optional[Dict[str, Any]]) -> str:
    """Prompt block with the fetched facts, or "" when nothing could be fetched."""
    if not is_grounded(context):
        return ""
    parts = []
    if context.get("article"):
        parts.append(f"Article context (lead of the article):\n{context['article']}")
    if context.get("added"):
        parts.append("Text added by the latest edits:\n" + "\n".join(f"- {text}" for text in context["added"]))
    return "\n\n".join(parts) + "\n\n"
