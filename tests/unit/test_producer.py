"""SSE producer: event validation and Kafka back-pressure handling."""

from unittest.mock import MagicMock

import pytest

import producer


@pytest.mark.parametrize(
    "change, expected",
    [
        ({"meta": {"domain": "en.wikipedia.org"}, "id": 1, "title": "Artemis II"}, True),
        ({"meta": "not-a-dict", "id": 1, "title": "x"}, False),
        ({"meta": {}, "title": "x"}, False),
        ({"meta": {}, "id": 1}, False),
        ("not a dict", False),
    ],
)
def test_is_valid_change(change, expected):
    assert producer.is_valid_change(change) is expected


def test_full_queue_retries_the_same_event_instead_of_dropping_it():
    kafka = MagicMock()
    kafka.produce.side_effect = [BufferError(), BufferError(), None]

    assert producer.produce_with_backpressure(kafka, "wiki-raw", b"{}", max_attempts=5) is True
    assert kafka.produce.call_count == 3
    kafka.poll.assert_any_call(1)  # waited for deliveries to free the queue


def test_gives_up_after_max_attempts():
    kafka = MagicMock()
    kafka.produce.side_effect = BufferError()

    assert producer.produce_with_backpressure(kafka, "wiki-raw", b"{}", max_attempts=3) is False
    assert kafka.produce.call_count == 3


def test_user_agent_identifies_the_project():
    assert "github.com/JesusJimenez01/wiki-stream-mlops" in producer.USER_AGENT
