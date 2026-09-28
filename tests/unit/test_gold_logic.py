"""Gold layer: LLM output handling, editorial guardrails, deduplication and story lifecycle."""

import json
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

import gold_enrichment as gold

NOW = datetime(2026, 9, 28, 12, 0, tzinfo=timezone.utc)
SAMPLES = [{"title": "Artemis II", "comment": "crew update", "domain": "en.wikipedia.org"}]


def _candidate(minutes_ago, headline, summary="", topic_event_count=10, story_id="story-1", update_seq=1):
    return {
        "topic_event_count": topic_event_count,
        "headline": headline,
        "summary": summary,
        "gold_ts": (NOW - timedelta(minutes=minutes_ago)).isoformat(),
        "story_id": story_id,
        "published_at": (NOW - timedelta(minutes=minutes_ago)).isoformat(),
        "update_seq": update_seq,
    }


class TestModelOutput:
    def test_extract_json_object_plain_and_wrapped(self):
        assert gold.extract_json_object('{"headline": "x"}') == {"headline": "x"}
        assert gold.extract_json_object('Sure! {"headline": "x"} Hope it helps') == {"headline": "x"}

    @pytest.mark.parametrize("text", ["no json here", "[1, 2, 3]"])
    def test_extract_json_object_rejects_non_objects(self, text):
        with pytest.raises(ValueError):
            gold.extract_json_object(text)

    def test_normalize_model_output_accepts_comma_separated_tags(self):
        label, headline, summary, tags = gold.normalize_model_output(
            {"topic_label": "Space", "headline": "Crew named", "summary": "", "tags": "NASA, Moon"}
        )
        assert (label, headline, tags) == ("Space", "Crew named", ["NASA", "Moon"])
        assert summary  # empty summaries get a neutral fallback


class TestHumanFraming:
    def test_abstract_headline_is_rewritten_around_the_protagonist(self):
        _, headline, _, _ = gold.enforce_human_framing(
            "Artemis II", SAMPLES, "Space", "Update on several articles", "Crew confirmed for the flyby.", ["NASA"]
        )
        assert headline.startswith("Artemis II")

    def test_foreign_script_and_robotic_summaries_are_replaced(self):
        label, _, summary, tags = gold.enforce_human_framing(
            "Artemis II", SAMPLES, "Москва", "Artemis II crew named", "Москва новости", []
        )
        assert label == "News"
        assert "Artemis II" in summary
        assert tags == ["news"]

    def test_namespace_titles_are_not_used_as_protagonist(self):
        samples = [{"title": "Category:Spaceflight"}, {"title": "Artemis II"}]
        assert gold.pick_story_anchor("space", samples) == "Artemis II"


class TestCallOllama:
    def test_success_returns_structured_story(self, monkeypatch):
        body = {
            "response": json.dumps(
                {
                    "topic_label": "Space",
                    "headline": "Artemis II crew named",
                    "summary": "NASA confirmed the crew.",
                    "tags": ["NASA", "Moon"],
                }
            )
        }
        response = MagicMock()
        response.read.return_value = json.dumps(body).encode()
        opener = MagicMock()
        opener.return_value.__enter__.return_value = response
        monkeypatch.setattr(gold, "urlopen", opener)

        label, headline, summary, tags, ok, error = gold.call_ollama("prompt", "Artemis II", SAMPLES)

        assert ok is True and error is None
        assert headline == "Artemis II crew named"
        assert tags == ["NASA", "Moon"]

    @pytest.mark.parametrize("exc", [ConnectionResetError("reset by peer"), TimeoutError("timed out")])
    def test_network_errors_produce_a_fallback_instead_of_failing_the_batch(self, monkeypatch, exc):
        monkeypatch.setattr(gold, "urlopen", MagicMock(side_effect=exc))

        _, headline, summary, tags, ok, error = gold.call_ollama("prompt", "Artemis II", SAMPLES)

        assert ok is False
        assert str(exc) in error
        assert headline and summary and tags == ["fallback"]


class TestDeduplication:
    def test_similarity_helpers(self):
        assert gold.jaccard_similarity(set(), set()) == 1.0
        assert gold.jaccard_similarity({"moon"}, set()) == 0.0
        assert gold.jaccard_similarity({"moon", "crew"}, {"moon", "crew"}) == 1.0
        assert gold.build_similarity_tokens("The Moon crew, with NASA!") == {"moon", "crew", "nasa"}

    def test_recent_publication_blocks_the_topic_before_inference(self):
        recent = [_candidate(10, "Artemis II crew named")]
        old = [_candidate(120, "Artemis II crew named")]

        assert gold.find_recent_publication(recent, NOW) == recent[0]["gold_ts"]
        assert gold.find_recent_publication(old, NOW) is None
        assert gold.find_recent_publication([], NOW) is None

    def test_no_history_means_new_story(self):
        assert gold.evaluate_duplicate_news(10, "Artemis II crew named", "", [], NOW) == (False, 0.0, None, False)

    def test_recent_story_is_a_duplicate(self):
        is_dup, score, _, is_update = gold.evaluate_duplicate_news(
            30, "Completely different headline", "", [_candidate(10, "Artemis II crew named")], NOW
        )
        assert is_dup is True and score == 1.0 and is_update is False

    def test_similar_story_with_new_activity_becomes_an_update(self):
        candidates = [_candidate(120, "Artemis II crew named for lunar flyby", topic_event_count=10)]
        is_dup, _, _, is_update = gold.evaluate_duplicate_news(
            20, "Artemis II crew named for lunar flyby", "", candidates, NOW
        )
        assert is_dup is False and is_update is True

    def test_similar_story_without_new_activity_is_a_duplicate(self):
        candidates = [_candidate(120, "Artemis II crew named for lunar flyby", topic_event_count=10)]
        is_dup, score, _, _ = gold.evaluate_duplicate_news(
            11, "Artemis II crew named for lunar flyby", "", candidates, NOW
        )
        assert is_dup is True and score >= gold.GOLD_HEADLINE_SIM_THRESHOLD

    def test_different_story_on_same_topic_is_new(self):
        candidates = [
            _candidate(120, "Artemis II crew named for lunar flyby", "NASA confirmed four astronauts for the mission.")
        ]
        is_dup, _, _, is_update = gold.evaluate_duplicate_news(
            11,
            "Orion heat shield passes final review",
            "Engineers cleared the capsule after months of thermal testing.",
            candidates,
            NOW,
        )
        assert is_dup is False and is_update is False


class TestStoryLifecycle:
    def test_update_keeps_story_id_and_increments_sequence(self):
        info = gold.resolve_story_info([_candidate(120, "x", story_id="abc", update_seq=2)], True, NOW)
        assert info["story_id"] == "abc"
        assert info["update_seq"] == 3
        assert info["is_live_event"] is True

    def test_new_story_gets_fresh_identity(self):
        info = gold.resolve_story_info([], False, NOW)
        assert info["update_seq"] == 1
        assert info["is_live_event"] is False
        assert info["published_at"] == NOW.isoformat()

    def test_should_allow_update_requires_time_and_activity(self):
        two_hours_ago = (NOW - timedelta(hours=2)).isoformat()
        assert gold.should_allow_update(NOW, two_hours_ago, 10, 20) is True
        assert gold.should_allow_update(NOW, two_hours_ago, 10, 11) is False
        assert gold.should_allow_update(NOW, (NOW - timedelta(minutes=5)).isoformat(), 10, 50) is False
        assert gold.should_allow_update(NOW, None, 10, 50) is False


def test_prompt_contains_samples_and_disables_qwen_thinking():
    prompt = gold.build_topic_prompt("Artemis II", 12, SAMPLES)
    assert "Recurring topic: Artemis II" in prompt
    assert "title: Artemis II | comment: crew update" in prompt
    if gold.OLLAMA_MODEL.lower().startswith("qwen3"):
        assert prompt.startswith("/no_think")
