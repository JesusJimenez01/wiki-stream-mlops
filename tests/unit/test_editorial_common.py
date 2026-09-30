"""Editorial rules shared by Silver, Gold and the newsroom API."""

import pytest

from common.editorial_common import (
    ChangeFilter,
    burst_score,
    contains_foreign_script,
    informative_tags,
    is_namespace_like,
    label_quality_bonus,
    looks_like_generic_topic,
    looks_like_low_signal_topic,
    parse_csv_setting,
    rank_bursts,
    resolve_display_topic_label,
    resolve_source_label,
    sanitize_topic_label,
    source_label_from_domain,
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


def test_label_quality_prefers_specific_titles():
    specific = label_quality_bonus("2026 FIFA World Cup")
    generic = label_quality_bonus("history")

    assert specific > label_quality_bonus("football") > generic
    # A namespace prefix sinks an otherwise identical topic
    assert label_quality_bonus("Category:Sports") < label_quality_bonus("Sports")


def test_burst_score_rewards_many_editors_over_many_saves():
    crowd = burst_score("Hurricane Milton", editors=6, edits=8)
    one_person_saving = burst_score("Hurricane Milton", editors=1, edits=30)

    assert crowd > one_person_saving


def test_burst_score_bounds_volume_and_size_bonuses():
    base = burst_score("Artemis II", editors=3, edits=30, bytes_added=6000)

    assert burst_score("Artemis II", editors=3, edits=500, bytes_added=10**7) == base
    assert burst_score("Artemis II", editors=3, edits=30, bytes_added=-5000) < base


def test_rank_bursts_breaks_ties_by_editors_then_edits():
    topics = [
        {"label": "A", "score": 10.0, "editors": 3, "count": 9},
        {"label": "B", "score": 10.0, "editors": 4, "count": 5},
        {"label": "C", "score": 12.0, "editors": 3, "count": 5},
    ]

    assert [topic["label"] for topic in rank_bursts(topics)] == ["C", "B", "A"]


@pytest.mark.parametrize(
    "value, expected",
    [
        ("enwiki, eswiki", {"enwiki", "eswiki"}),
        ("", set()),
        (None, set()),
        ("*", set()),
        ("enwiki,*", set()),
    ],
)
def test_parse_csv_setting(value, expected):
    assert parse_csv_setting(value) == expected


def _change(**overrides):
    change = {"wiki": "enwiki", "type": "edit", "namespace": 0, "bot": False, "minor": False}
    change.update(overrides)
    return change


@pytest.mark.parametrize(
    "overrides, accepted",
    [
        ({}, True),
        ({"type": "new"}, True),
        ({"bot": True}, False),
        ({"minor": True}, False),
        ({"wiki": "wikidatawiki"}, False),
        ({"type": "categorize"}, False),
        ({"type": "log", "namespace": -1}, False),
        ({"namespace": 14}, False),  # Category:
        ({"namespace": None}, False),
    ],
)
def test_default_change_filter_keeps_human_article_edits_on_english_wikipedia(overrides, accepted):
    assert ChangeFilter().accepts(_change(**overrides)) is accepted


def test_change_filter_from_settings_with_wildcards():
    change_filter = ChangeFilter.from_settings(wikis="enwiki,eswiki", types="*", namespaces="0, 118")

    assert change_filter.namespaces == {0, 118}
    assert change_filter.accepts(_change(wiki="eswiki", type="log", namespace=118))
    assert not change_filter.accepts(_change(wiki="dewiki"))
    assert ChangeFilter.from_settings("*", "*", "*").accepts(_change(wiki="wikidatawiki", namespace=120))


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
