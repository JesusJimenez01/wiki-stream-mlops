"""Offline evaluation tools: SSE recording, selection settings and precision scoring."""

import io
import json
from datetime import datetime, timezone

import pytest

import offline_topics
import record_stream


def test_sse_parser_joins_multiline_data_and_tracks_the_last_event_id():
    stream = [
        ": keep-alive\n",
        "event: message\n",
        'id: [{"offset": 1}]\n',
        'data: {"a":\n',
        "data: 1}\n",
        "\n",
        "\n",
        "data:no-space\n",
        "\r\n",
        "data: trailing",
    ]

    events = list(record_stream.iter_sse_events(stream))

    assert events == [
        ('[{"offset": 1}]', '{"a":\n1}'),
        ('[{"offset": 1}]', "no-space"),
        ('[{"offset": 1}]', "trailing"),
    ]


def test_parse_change_filters_by_wiki_and_skips_invalid_payloads():
    change = {"wiki": "enwiki", "meta": {"id": "x"}}

    assert record_stream.parse_change(json.dumps(change), set()) == change
    assert record_stream.parse_change(json.dumps(change), {"eswiki"}) is None
    assert record_stream.parse_change("not json", set()) is None
    assert record_stream.parse_change('{"no": "meta"}', set()) is None


def test_record_writes_one_json_line_per_kept_event(monkeypatch):
    body = [
        b'data: {"wiki": "enwiki", "meta": {}, "title": "A"}\n',
        b"\n",
        b'data: {"wiki": "dewiki", "meta": {}, "title": "B"}\n',
        b"\n",
        b'data: {"wiki": "enwiki", "meta": {}, "title": "C"}\n',
        b"\n",
    ]

    class FakeResponse:
        def __enter__(self):
            return iter(body)

        def __exit__(self, *exc):
            return False

    monkeypatch.setattr(record_stream, "urlopen", lambda request, timeout: FakeResponse())
    out = io.StringIO()

    written = record_stream.record("https://stream", out, seconds=60, wikis={"enwiki"}, max_events=2)

    assert written == 2
    assert [json.loads(line)["title"] for line in out.getvalue().splitlines()] == ["A", "C"]


def _args(argv):
    return offline_topics.build_parser().parse_args(["select", "--input", "x.jsonl", "--out", "y.csv", *argv])


def test_selection_config_defaults_come_from_the_environment(monkeypatch):
    monkeypatch.setenv("SILVER_TOPIC_MIN_EDITORS", "4")
    monkeypatch.setenv("SILVER_ALLOWED_WIKIS", "eswiki")

    config = offline_topics.selection_config(_args([]))

    assert config.min_editors == 4
    assert config.change_filter.wikis == {"eswiki"}
    assert config.change_filter.namespaces == {0}


def test_selection_config_cli_overrides_emulate_the_legacy_selection(monkeypatch):
    monkeypatch.delenv("SILVER_ALLOWED_WIKIS", raising=False)

    config = offline_topics.selection_config(_args(["--wikis", "*", "--types", "*", "--min-editors", "1"]))

    assert config.change_filter.wikis == frozenset() and config.change_filter.types == frozenset()
    assert config.change_filter.namespaces == {0}  # not overridden
    assert config.min_editors == 1


def _record(key, rank_editors, score):
    return {
        "topic_key": key,
        "topic_label": key.title(),
        "wiki": "enwiki",
        "topic_editor_count": rank_editors,
        "topic_event_count": rank_editors * 2,
        "topic_bytes_added": 100,
        "topic_score": score,
        "title_url": f"https://en.wikipedia.org/wiki/{key}",
        "samples_json": json.dumps([{"comment": "c1"}, {"comment": ""}, {"comment": "c2"}]),
    }


def test_merge_selection_keeps_first_sighting_and_peaks():
    selected = {}
    first = datetime(2026, 9, 30, 12, 0, tzinfo=timezone.utc)
    later = datetime(2026, 9, 30, 12, 5, tzinfo=timezone.utc)

    offline_topics.merge_selection(selected, [_record("storm", 3, 10.0), _record("moon", 4, 12.0)], first)
    offline_topics.merge_selection(selected, [_record("moon", 6, 20.0)], later)

    moon = selected["moon"]
    assert moon["first_selected_at"] == "2026-09-30T12:00+00:00"
    assert (moon["windows_selected"], moon["best_rank"], moon["peak_editors"], moon["peak_score"]) == (2, 1, 6, 20.0)
    assert moon["example_comments"] == "c1 | c2"
    assert selected["storm"]["windows_selected"] == 1


@pytest.mark.parametrize(
    "value, expected",
    [("y", True), ("Sí", True), (" X ", True), ("1", True), ("n", False), ("no", False), ("", None), ("maybe", None)],
)
def test_parse_label(value, expected):
    assert offline_topics.parse_label(value) is expected


def test_precision_report_ranks_by_score():
    rows = [
        {"topic_key": "a", "peak_score": "30", "newsworthy": "y"},
        {"topic_key": "b", "peak_score": "20", "newsworthy": "n"},
        {"topic_key": "c", "peak_score": "10", "newsworthy": "y"},
        {"topic_key": "d", "peak_score": "5", "newsworthy": "n"},
        {"topic_key": "e", "peak_score": "50", "newsworthy": ""},
    ]

    report = offline_topics.precision_report(rows, k=2)

    assert report == {"rows": 5, "labelled": 4, "newsworthy": 2, "precision": 0.5, "k": 2, "precision_at_k": 0.5}
    assert offline_topics.precision_report([], k=5)["precision"] is None


def test_csv_round_trip_and_semicolon_files(tmp_path):
    path = tmp_path / "candidates.csv"
    offline_topics.write_rows(str(path), [{"topic_key": "café", "topic_label": "Café", "newsworthy": "y"}])

    assert offline_topics.read_rows(str(path))[0]["topic_key"] == "café"

    excel = tmp_path / "excel.csv"
    excel.write_text("topic_key;peak_score;newsworthy\nmoon;12;y\nstorm;3;n\n", encoding="utf-8-sig")
    assert offline_topics.precision_report(offline_topics.read_rows(str(excel)), k=1)["precision_at_k"] == 1.0


def test_score_applies_existing_labels_to_another_configuration(tmp_path, capsys):
    labels = tmp_path / "labels.csv"
    other = tmp_path / "other.csv"
    offline_topics.write_rows(
        str(labels),
        [{"topic_key": "moon", "peak_score": 9, "newsworthy": "y"}, {"topic_key": "sandbox", "newsworthy": "n"}],
    )
    offline_topics.write_rows(str(other), [{"topic_key": "sandbox", "peak_score": 3}, {"topic_key": "new"}])

    offline_topics.main(["score", "--labels", str(labels), "--candidates", str(other), "--k", "5"])

    output = capsys.readouterr().out
    assert "precision: 50%" in output
    assert "precision: 0%" in output
    assert "(1 of its articles have no label yet)" in output
