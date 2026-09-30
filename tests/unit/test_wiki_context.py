"""Grounding context from the Wikimedia REST API (HTTP is mocked)."""

import pytest

from common import wiki_context as wc


class FakeApi:
    """Stands in for fetch_json: maps URL suffixes to payloads or exceptions."""

    def __init__(self, routes):
        self.routes = routes
        self.calls = []

    def __call__(self, url):
        self.calls.append(url)
        for suffix, result in self.routes.items():
            if url.endswith(suffix):
                if isinstance(result, Exception):
                    raise result
                return result
        raise OSError(f"404 {url}")


@pytest.mark.parametrize("host", ["en.wikipedia.org", "www.wikidata.org", "commons.wikimedia.org", "EN.WIKIPEDIA.ORG"])
def test_wikimedia_hosts_are_accepted(host):
    assert wc.wiki_host(host) == host.lower()


@pytest.mark.parametrize(
    "host", ["", None, "evil.com", "en.wikipedia.org.evil.com", "169.254.169.254", "a/b.wikipedia.org"]
)
def test_other_hosts_are_rejected(host):
    with pytest.raises(ValueError):
        wc.wiki_host(host)


def test_page_summary_encodes_the_title_and_returns_the_extract():
    api = FakeApi({"/page/summary/AC%2FDC_%28band%29": {"extract": "  AC/DC are an Australian rock band. "}})

    assert wc.page_summary("en.wikipedia.org", "AC/DC (band)", api) == "AC/DC are an Australian rock band."
    assert api.calls == ["https://en.wikipedia.org/api/rest_v1/page/summary/AC%2FDC_%28band%29"]


def test_page_summary_tolerates_unexpected_payloads():
    assert wc.page_summary("en.wikipedia.org", "X", FakeApi({"/X": ["not", "a", "dict"]})) == ""
    assert wc.page_summary("en.wikipedia.org", "   ", FakeApi({})) == ""


def test_added_text_keeps_added_lines_and_added_highlights_only():
    changed = "Café opened in 2024 2025."
    added_start = len("Café opened in 2024 ".encode())  # offsets are UTF-8 bytes, not characters
    payload = {
        "diff": [
            {"type": 0, "text": "Unchanged context."},
            {"type": 1, "text": "The '''crew''' was [[NASA|announced]] on 3 April.<ref>{{cite web|url=x}}</ref>"},
            {"type": 2, "text": "A deleted line."},
            {
                "type": 3,
                "text": changed,
                "highlightRanges": [
                    {"start": len("Café opened in ".encode()), "length": 4, "type": 1},  # deleted "2024"
                    {"start": added_start, "length": 4, "type": 0},  # added "2025"
                ],
            },
            "garbage",
        ]
    }

    assert wc.added_text_from_diff(payload) == "The crew was announced on 3 April. 2025"


def test_added_text_calls_the_compare_endpoint():
    api = FakeApi({"/revision/10/compare/11": {"diff": [{"type": 1, "text": "New fact."}]}})

    assert wc.added_text("en.wikipedia.org", "10", 11, api) == "New fact."
    assert api.calls == ["https://en.wikipedia.org/w/rest.php/v1/revision/10/compare/11"]


def test_clean_wikitext_strips_markup():
    text = (
        "== Career ==\n* '''Bold''' [[Target|label]] and [[Plain]]<!-- hidden --> "
        "{{Infobox|name={{nested}}}} [https://example.org Example] [[File:X.jpg|thumb]] <br/>end"
    )

    assert wc.clean_wikitext(text) == "Career Bold label and Plain Example end"


def _sample(rev_new, byte_delta, rev_old=None):
    return {
        "title": "Artemis II",
        "server_name": "en.wikipedia.org",
        "rev_old": rev_old if rev_old is not None else rev_new - 1,
        "rev_new": rev_new,
        "byte_delta": byte_delta,
    }


def test_topic_context_uses_the_largest_edits_and_survives_api_failures():
    api = FakeApi(
        {
            "/page/summary/Artemis_II": OSError("timed out"),
            "/compare/200": {"diff": [{"type": 1, "text": "Crew announced."}]},
            "/compare/300": ValueError("bad JSON"),
            "/compare/100": {"diff": [{"type": 1, "text": "Tiny fix."}]},
        }
    )
    samples = [_sample(100, 5), _sample(200, 900), _sample(300, 400), {"title": "Artemis II", "rev_new": 400}]

    context = wc.build_topic_context(samples, max_diffs=2, fetch=api)

    assert context == {"article": "", "added": ["Crew announced."]}
    assert not any(call.endswith("/compare/100") for call in api.calls)  # only the 2 largest edits
    assert wc.is_grounded(context)


def test_topic_context_respects_the_character_budget():
    api = FakeApi(
        {
            "/page/summary/Artemis_II": {"extract": "A" * 50},
            "/compare/200": {"diff": [{"type": 1, "text": "B" * 50}]},
            "/compare/100": {"diff": [{"type": 1, "text": "C" * 50}]},
        }
    )

    context = wc.build_topic_context([_sample(100, 5), _sample(200, 900)], max_diffs=3, max_chars=30, fetch=api)

    assert context["article"] == "A" * 30
    assert context["added"] == ["B" * 30]


def test_render_context_is_empty_without_facts():
    assert wc.render_context(None) == ""
    assert wc.render_context({"article": "", "added": []}) == ""
    assert not wc.is_grounded({"article": "", "added": []})
    assert wc.build_topic_context([], fetch=FakeApi({})) == {"article": "", "added": []}

    rendered = wc.render_context({"article": "Lead.", "added": ["One.", "Two."]})
    assert "Article context (lead of the article):\nLead." in rendered
    assert "Text added by the latest edits:\n- One.\n- Two." in rendered
