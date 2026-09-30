"""Pure-Python editorial rules shared by the Spark jobs and the newsroom API."""

import re
from dataclasses import dataclass
from typing import Any, Dict, FrozenSet, List, Optional, Sequence

KNOWN_NAMESPACE_PREFIXES = (
    "category",
    "categories",
    "template",
    "template talk",
    "special",
    "user",
    "user talk",
    "module",
    "wikipedia",
    "wikipedia talk",
    "help",
    "help talk",
    "portal",
    "draft",
    "file",
    "talk",
    "mediawiki",
    "timedtext",
    "categoria",
    "categoría",
    "categorie",
    "catégorie",
    "kategorie",
    "kategori",
    "luokka",
    "kateqoriya",
    "plantilla",
    "usuario",
    "usuario discusión",
    "usuario discusion",
    "utente",
    "discussion utilisateur",
    "utilisateur",
    "modulo",
    "módulo",
    "ayuda",
    "archivo",
    "anexo",
    "discusion",
    "discusión",
    "discussione",
    "discussion",
    "wiktionary",
    "wiktionnaire",
)

GENERIC_NAMESPACE_REGEX = r"^[^\s:]{1,30}:"
TITLE_NAMESPACE_REGEX = r"^(?:" + "|".join(re.escape(prefix) for prefix in KNOWN_NAMESPACE_PREFIXES) + r"):"
FOREIGN_SCRIPT_REGEX = (
    r"[\u0400-\u052F\u0590-\u05FF\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF\u0900-\u0D7F\u3040-\u30FF\u3400-\u9FFF]"
)
EDITORIAL_NOISE_REGEX = (
    r"(^|[^a-z0-9])(q\d+|p\d+|special|create|property|batch|short|removed|"
    r"quickstatements|wbeditentity|toollabs|wikidata|wikimedia|commons)([^a-z0-9]|$)"
)

LOW_SIGNAL_TOPIC_TERMS = {
    "talk",
    "user talk",
    "sandbox",
    "draft",
    "template",
    "template talk",
    "special",
    "wikipedia",
    "wiktionary",
    "wiktionnaire",
    "category",
    "categories",
    "categoria",
    "categoría",
    "categorie",
    "catégorie",
    "kategorie",
    "kategori",
    "luokka",
    "kateqoriya",
    "discussion",
    "discusion",
    "discusión",
    "discussione",
    "utente",
    "user",
    "portal",
    "project",
    "stub",
    "maintenance",
    "articles lacking sources",
    "articles without sources",
    "unreferenced",
    "referencias",
}

GENERIC_TOPIC_TERMS = {
    "history",
    "historical",
    "article",
    "articles",
    "entry",
    "entries",
    "page",
    "pages",
    "list",
    "lists",
    "project",
    "projects",
    "content",
    "contents",
    "data",
    "record",
    "records",
    "resource",
    "resources",
    "categoria",
    "categoría",
    "category",
    "categories",
    "dictionarys",
    "diccionario",
    "diccionarios",
    "historia",
    "items",
}

LOW_SIGNAL_TOPIC_REGEX = (
    r"(^|[^a-z0-9])(talk|user talk|sandbox|draft|template(?: talk)?|special|wikipedia|wiktionary|"
    r"wiktionnaire|category|categories|categoria|categoría|categorie|catégorie|kategorie|kategori|"
    r"luokka|kateqoriya|discussion|discusion|discusión|discussione|utente|portal|project|stub|maintenance|"
    r"unreferenced|referencias)([^a-z0-9]|$)"
)

GENERIC_TAGS = {
    "wikipedia",
    "wikidata",
    "wikimedia",
    "updates",
    "update",
    "general",
    "international",
    "news",
}


def contains_foreign_script(text: Any) -> bool:
    if not text:
        return False
    return bool(re.search(FOREIGN_SCRIPT_REGEX, str(text)))


def is_namespace_like(text: Any) -> bool:
    if not text:
        return False
    normalized = str(text).strip().lower()
    return bool(re.match(TITLE_NAMESPACE_REGEX, normalized) or re.match(GENERIC_NAMESPACE_REGEX, normalized))


def looks_like_low_signal_topic(text: Any) -> bool:
    if not text:
        return False
    normalized = str(text).strip().lower()
    compact = re.sub(r"\s+", " ", normalized)
    if compact in LOW_SIGNAL_TOPIC_TERMS:
        return True
    if is_namespace_like(compact):
        return True
    return bool(re.search(LOW_SIGNAL_TOPIC_REGEX, compact))


def looks_like_generic_topic(text: Any) -> bool:
    if not text:
        return False
    normalized = re.sub(r"\s+", " ", str(text).strip().lower())
    return normalized in GENERIC_TOPIC_TERMS


# ---------------------------------------------------------------------------
# Structured selection of recent changes
# ---------------------------------------------------------------------------


def parse_csv_setting(value: Optional[str]) -> FrozenSet[str]:
    """
    Parse a comma-separated setting. An empty value or "*" means "no restriction"
    and is returned as an empty set.
    """
    items = {item.strip() for item in (value or "").split(",") if item.strip()}
    return frozenset() if "*" in items else frozenset(items)


@dataclass(frozen=True)
class ChangeFilter:
    """
    Which Wikimedia recent-change events are eligible to become news.

    Uses the structured fields of the event (``wiki``, ``type``, ``namespace``)
    instead of guessing from the title: namespace 0 is the article namespace in
    every language, and ``type`` separates real edits from categorisation and
    log events. An empty set means "accept any value".
    """

    wikis: FrozenSet[str] = frozenset({"enwiki"})
    types: FrozenSet[str] = frozenset({"edit", "new"})
    namespaces: FrozenSet[int] = frozenset({0})

    @classmethod
    def from_settings(cls, wikis: str, types: str, namespaces: str) -> "ChangeFilter":
        return cls(
            wikis=parse_csv_setting(wikis),
            types=parse_csv_setting(types),
            namespaces=frozenset(int(ns) for ns in parse_csv_setting(namespaces)),
        )

    def accepts(self, change: Dict[str, Any]) -> bool:
        """Pure-Python twin of the Spark filter, used by the offline tools and tests."""
        if change.get("bot") or change.get("minor"):
            return False
        if self.wikis and change.get("wiki") not in self.wikis:
            return False
        if self.types and change.get("type") not in self.types:
            return False
        if self.namespaces and change.get("namespace") not in self.namespaces:
            return False
        return True


def burst_score(label: Any, editors: int, edits: int, bytes_added: int = 0) -> float:
    """
    Newsworthiness score of an editing burst on one article.

    Distinct editors dominate: several people editing the same article at once is
    the classic breaking-news signal, while one person saving many times is not.
    Edit volume and added content add a bounded bonus, and the title quality
    adjustment demotes generic or maintenance topics.
    """
    score = 3.0 * editors + 0.5 * min(edits, 30) + min(max(bytes_added, 0) / 2000.0, 3.0)
    return score + label_quality_bonus(label)


def rank_bursts(topics: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Order burst candidates by score, then editors, edits and label length."""
    return sorted(
        topics,
        key=lambda item: (item["score"], item["editors"], item["count"], len(item["label"])),
        reverse=True,
    )


def label_quality_bonus(label: Any) -> float:
    """Title heuristics: specific, proper-noun titles beat generic or maintenance ones."""
    raw_label = str(label or "").strip()
    normalized = re.sub(r"\s+", " ", raw_label.lower())
    tokens = [token for token in re.split(r"[^\wÀ-ÿ]+", raw_label) if token]

    score = 0.0
    if len(tokens) >= 2:
        score += 2.0
    if any(char.isdigit() for char in raw_label):
        score += 1.5
    if any(char.isupper() for char in raw_label[:1]):
        score += 1.0
    if len(raw_label) >= 10:
        score += 1.0
    if looks_like_generic_topic(normalized):
        score -= 4.0
    if looks_like_low_signal_topic(normalized):
        score -= 8.0

    return score


def source_label_from_domain(domain: Optional[str]) -> str:
    domain_text = (domain or "").strip().lower()
    if "wikidata.org" in domain_text:
        return "Wikidata"
    if "commons.wikimedia.org" in domain_text:
        return "Wikimedia Commons"
    if "wikipedia.org" in domain_text:
        return "Wikipedia"
    if "wikimedia.org" in domain_text:
        return "Wikimedia"
    return "Wikimedia"


def resolve_source_label(samples: List[Dict[str, str]]) -> str:
    counts: Dict[str, int] = {}
    for sample in samples:
        label = source_label_from_domain(sample.get("domain"))
        counts[label] = counts.get(label, 0) + 1
    return max(counts, key=counts.get) if counts else "Wikimedia"


def informative_tags(tags: Any) -> List[str]:
    clean_tags: List[str] = []
    for tag in tags or []:
        value = str(tag or "").strip()
        if not value or contains_foreign_script(value) or value.lower() in GENERIC_TAGS:
            continue
        clean_tags.append(value)
    return clean_tags


def sanitize_topic_label(topic_term: Any, tags: Any, domain: Any) -> str:
    raw = str(topic_term or "").strip()
    if raw and not looks_like_low_signal_topic(raw):
        return raw

    tags_clean = informative_tags(tags)
    if len(tags_clean) >= 2:
        return f"{tags_clean[0]} / {tags_clean[1]}"
    if len(tags_clean) == 1:
        return tags_clean[0]
    return source_label_from_domain(str(domain or ""))


def resolve_display_topic_label(
    topic_label: Any,
    topic_term: Any,
    tags: Any,
    domain: Any,
    headline: Any = None,
) -> str:
    for candidate in [topic_label, sanitize_topic_label(topic_term, tags, domain)]:
        value = str(candidate or "").strip()
        if value and not contains_foreign_script(value) and not looks_like_low_signal_topic(value):
            return value

    tags_clean = informative_tags(tags)
    if len(tags_clean) >= 2:
        return f"{tags_clean[0]} / {tags_clean[1]}"
    if len(tags_clean) == 1:
        return tags_clean[0]

    headline_text = re.sub(r"\s+", " ", str(headline or "").strip())
    if headline_text and not contains_foreign_script(headline_text):
        return " ".join(headline_text.split()[:4])

    return source_label_from_domain(str(domain or ""))
