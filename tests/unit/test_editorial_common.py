"""Editorial rules shared by Silver, Gold and the newsroom API."""

import pytest

from common.editorial_common import (
    contains_foreign_script,
    informative_tags,
    is_namespace_like,
    looks_like_generic_topic,
    looks_like_low_signal_topic,
    resolve_display_topic_label,
    resolve_source_label,
    sanitize_topic_label,
    source_label_from_domain,
    topic_rank_score,
)


@pytest.mark.parametrize(
    "text, expected",
    [
        ("Artemis II", False),
        ("Café Society", False),
        ("Москва", True),
        ("東京", True),
        ("", False),
        (None, False),
    ],
)
def test_contains_foreign_script(text, expected):
    assert contains_foreign_script(text) is expected


@pytest.mark.parametrize(
    "title, expected",
    [
        ("Category:Living people", True),
        ("Categoría:Ciencia", True),
        ("User talk:Example", True),
        ("Artemis II", False),
        ("Star Wars: Episode IV", False),  # a colon after a space is part of a real title
    ],
)
def test_is_namespace_like(title, expected):
    assert is_namespace_like(title) is expected


@pytest.mark.parametrize(
    "topic, expected",
    [
        ("sandbox", True),
        ("Template:Infobox", True),
        ("articles lacking sources", True),
        ("Artemis II", False),
        ("2026 FIFA World Cup", False),
    ],
)
def test_looks_like_low_signal_topic(topic, expected):
    assert looks_like_low_signal_topic(topic) is expected


def test_generic_topics_are_detected_case_insensitively():
    assert looks_like_generic_topic("  History ")
    assert not looks_like_generic_topic("History of Rome")


def test_topic_rank_prefers_specific_exact_titles():
    specific = topic_rank_score("2026 FIFA World Cup", 10, "title_exact")
    token = topic_rank_score("football", 10, "token_contains")
    generic = topic_rank_score("history", 10, "title_exact")
    noise = topic_rank_score("Category:Sports", 10, "title_exact")

    assert specific > token
    assert specific > generic
    # A namespace prefix sinks an otherwise identical topic
    assert noise < topic_rank_score("Sports", 10, "title_exact")


def test_source_labels():
    assert source_label_from_domain("www.wikidata.org") == "Wikidata"
    assert source_label_from_domain("commons.wikimedia.org") == "Wikimedia Commons"
    assert source_label_from_domain("en.wikipedia.org") == "Wikipedia"
    assert source_label_from_domain(None) == "Wikimedia"

    samples = [{"domain": "en.wikipedia.org"}, {"domain": "es.wikipedia.org"}, {"domain": "www.wikidata.org"}]
    assert resolve_source_label(samples) == "Wikipedia"
    assert resolve_source_label([]) == "Wikimedia"


def test_informative_tags_drop_generic_and_foreign_values():
    assert informative_tags(["Wikipedia", "Space", "Москва", " ", "NASA"]) == ["Space", "NASA"]


def test_sanitize_topic_label_falls_back_to_tags_then_source():
    assert sanitize_topic_label("Artemis II", [], "en.wikipedia.org") == "Artemis II"
    assert sanitize_topic_label("sandbox", ["Space", "NASA"], "en.wikipedia.org") == "Space / NASA"
    assert sanitize_topic_label("sandbox", [], "www.wikidata.org") == "Wikidata"


def test_display_label_never_shows_low_signal_or_foreign_text():
    assert resolve_display_topic_label("Moon missions", "Artemis II", [], "en.wikipedia.org") == "Moon missions"
    assert resolve_display_topic_label("Москва", "Artemis II", [], "en.wikipedia.org") == "Artemis II"
    label = resolve_display_topic_label("", "sandbox", [], "en.wikipedia.org", headline="Crew named for lunar flyby")
    assert label == "Crew named for lunar"
