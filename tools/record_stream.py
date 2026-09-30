"""
Record a sample of the Wikimedia recent-changes stream to a JSON Lines file.

Standard library only, so it runs with any Python 3.10+ (or inside the Spark image):

    python tools/record_stream.py --minutes 60 --out data/recentchange.jsonl
    python tools/record_stream.py --minutes 30 --wikis enwiki,eswiki --out data/en-es.jsonl

The file feeds ``tools/offline_topics.py``, which replays it through the real
Silver selection so thresholds can be tuned without running the whole stack.
"""

import argparse
import http.client
import json
import os
import sys
import time
from typing import Iterable, Iterator, Optional, Set, TextIO, Tuple
from urllib.request import Request, urlopen

DEFAULT_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
USER_AGENT = os.getenv("WIKI_USER_AGENT", "wiki-stream-mlops/1.0 (https://github.com/JesusJimenez01/wiki-stream-mlops)")
RECONNECT_DELAY_SECONDS = 5


def iter_sse_events(lines: Iterable[str]) -> Iterator[Tuple[Optional[str], str]]:
    """Yield ``(last event id, data)`` for every Server-Sent Events message."""
    data_lines = []
    event_id: Optional[str] = None
    for raw_line in lines:
        line = raw_line.rstrip("\r\n")
        if not line:
            if data_lines:
                yield event_id, "\n".join(data_lines)
            data_lines = []
            continue
        if line.startswith(":"):  # comment / keep-alive
            continue
        field, _, value = line.partition(":")
        value = value[1:] if value.startswith(" ") else value
        if field == "data":
            data_lines.append(value)
        elif field == "id":
            event_id = value
    if data_lines:
        yield event_id, "\n".join(data_lines)


def parse_change(data: str, wikis: Set[str]) -> Optional[dict]:
    """Decode one event, keeping it only if it belongs to one of ``wikis`` (empty = all)."""
    try:
        change = json.loads(data)
    except ValueError:
        return None
    if not isinstance(change, dict) or "meta" not in change:
        return None
    if wikis and change.get("wiki") not in wikis:
        return None
    return change


def record(url: str, out: TextIO, seconds: float, wikis: Set[str], max_events: int) -> int:
    deadline = time.monotonic() + seconds
    last_event_id: Optional[str] = None
    written = 0

    def done() -> bool:
        return time.monotonic() >= deadline or (max_events > 0 and written >= max_events)

    while not done():
        headers = {"User-Agent": USER_AGENT, "Accept": "text/event-stream"}
        if last_event_id:
            headers["Last-Event-ID"] = last_event_id  # resume where the dropped connection stopped
        try:
            with urlopen(Request(url, headers=headers), timeout=60) as response:
                lines = (raw.decode("utf-8", errors="replace") for raw in response)
                for event_id, data in iter_sse_events(lines):
                    last_event_id = event_id or last_event_id
                    change = parse_change(data, wikis)
                    if change is not None:
                        out.write(json.dumps(change, ensure_ascii=False) + "\n")
                        written += 1
                        if written % 1000 == 0:
                            print(f"{written} events recorded", file=sys.stderr)
                    if done():
                        break
        except (OSError, http.client.HTTPException) as exc:
            print(f"Stream error: {exc}; reconnecting in {RECONNECT_DELAY_SECONDS}s", file=sys.stderr)
            time.sleep(RECONNECT_DELAY_SECONDS)
        except KeyboardInterrupt:
            print("Interrupted, keeping what was recorded", file=sys.stderr)
            break
    return written


def main(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out", required=True, help="JSON Lines file to append to")
    parser.add_argument("--minutes", type=float, default=60, help="how long to record (default: 60)")
    parser.add_argument("--max-events", type=int, default=0, help="stop after N events (default: no limit)")
    parser.add_argument("--wikis", default="", help="comma-separated wikis to keep, e.g. enwiki,eswiki (default: all)")
    parser.add_argument("--url", default=DEFAULT_URL, help="SSE endpoint")
    args = parser.parse_args(argv)

    wikis = {wiki.strip() for wiki in args.wikis.split(",") if wiki.strip()}
    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    with open(args.out, "a", encoding="utf-8") as out:
        written = record(args.url, out, args.minutes * 60, wikis, args.max_events)
    print(f"Done: {written} events appended to {args.out}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
