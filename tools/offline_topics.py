"""
Offline evaluation of the news selection.

``select`` replays recorded events through the *real* Silver code
(``transform_to_silver`` → ``publishable_events`` → ``build_topic_records``) in
sliding windows, as the streaming job would, and writes one CSV row per
selected article. Fill the ``newsworthy`` column (y/n) by hand, then ``score``
reports precision and precision@k. Any selection setting can be overridden to
compare configurations on the same sample.

    python tools/offline_topics.py select --input data/recentchange.jsonl --out data/candidates.csv
    python tools/offline_topics.py select --input data/recentchange.jsonl --out data/legacy.csv \\
        --wikis '*' --types '*' --min-editors 1 --min-edits 3
    python tools/offline_topics.py score --labels data/candidates.csv --k 10
    python tools/offline_topics.py score --labels data/candidates.csv --candidates data/legacy.csv

``select`` needs PySpark (and Java); inside Docker use the ``selection-lab``
service, e.g. ``docker compose run --rm selection-lab select ...``.
With ``--bronze`` it reads the Bronze Delta table in MinIO instead of a file.
"""

import argparse
import csv
import dataclasses
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

SPARK_JOBS_DIR = Path(__file__).resolve().parents[1] / "spark-jobs"
if str(SPARK_JOBS_DIR) not in sys.path:
    sys.path.insert(0, str(SPARK_JOBS_DIR))

CSV_COLUMNS = [
    "topic_key",
    "topic_label",
    "wiki",
    "first_selected_at",
    "windows_selected",
    "best_rank",
    "peak_editors",
    "peak_edits",
    "peak_bytes_added",
    "peak_score",
    "title_url",
    "example_comments",
    "newsworthy",
    "notes",
]
POSITIVE_LABELS = {"y", "yes", "s", "si", "sí", "1", "true", "x"}
NEGATIVE_LABELS = {"n", "no", "0", "false"}


# ---------------------------------------------------------------------------
# select
# ---------------------------------------------------------------------------


def selection_config(args: argparse.Namespace):
    """Settings from the environment (as the job would use), overridden by CLI flags."""
    from common.editorial_common import ChangeFilter
    from silver_processing import SelectionConfig

    config = SelectionConfig.from_env()
    change_filter = config.change_filter
    if any(value is not None for value in (args.wikis, args.types, args.namespaces)):
        change_filter = ChangeFilter.from_settings(
            wikis=args.wikis if args.wikis is not None else ",".join(sorted(change_filter.wikis)) or "*",
            types=args.types if args.types is not None else ",".join(sorted(change_filter.types)) or "*",
            namespaces=(
                args.namespaces
                if args.namespaces is not None
                else ",".join(str(ns) for ns in sorted(change_filter.namespaces)) or "*"
            ),
        )
    overrides = {
        name: getattr(args, name)
        for name in ("min_edits", "min_editors", "top_n", "lookback_minutes", "hold_minutes", "sample_size")
        if getattr(args, name) is not None
    }
    return dataclasses.replace(config, change_filter=change_filter, **overrides)


def merge_selection(selected: Dict[str, Dict[str, Any]], records: Iterable[Dict[str, Any]], now: datetime) -> None:
    """Fold one window's topic records into one row per article (first sighting + peaks)."""
    for rank, record in enumerate(records, start=1):
        row = selected.get(record["topic_key"])
        if row is None:
            samples = json.loads(record.get("samples_json") or "[]")
            comments = [sample.get("comment", "") for sample in samples if sample.get("comment")]
            row = selected[record["topic_key"]] = {
                "topic_key": record["topic_key"],
                "topic_label": record["topic_label"],
                "wiki": record.get("wiki") or "",
                "first_selected_at": now.isoformat(timespec="minutes"),
                "windows_selected": 0,
                "best_rank": rank,
                "peak_editors": 0,
                "peak_edits": 0,
                "peak_bytes_added": 0,
                "peak_score": 0.0,
                "title_url": record.get("title_url") or "",
                "example_comments": " | ".join(comments[:3]),
                "newsworthy": "",
                "notes": "",
            }
        row["windows_selected"] += 1
        row["best_rank"] = min(row["best_rank"], rank)
        row["peak_editors"] = max(row["peak_editors"], int(record.get("topic_editor_count") or 0))
        row["peak_edits"] = max(row["peak_edits"], int(record.get("topic_event_count") or 0))
        row["peak_bytes_added"] = max(row["peak_bytes_added"], int(record.get("topic_bytes_added") or 0))
        row["peak_score"] = round(max(row["peak_score"], float(record.get("topic_score") or 0.0)), 2)


def _as_utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def run_select(args: argparse.Namespace) -> int:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import current_timestamp
    from pyspark.sql.functions import max as spark_max
    from pyspark.sql.functions import min as spark_min

    import silver_processing as silver

    config = selection_config(args)
    print(f"Selection config: {config}", file=sys.stderr)

    if args.bronze:
        from common.spark_common import create_spark_session

        spark = create_spark_session("OfflineTopics")
        bronze_df = spark.read.format("delta").load(silver.BRONZE_PATH).select("raw_json")
    else:
        spark = SparkSession.builder.master(args.master).appName("OfflineTopics").getOrCreate()
        bronze_df = spark.read.text(args.input).withColumnRenamed("value", "raw_json")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.sparkContext.setLogLevel("WARN")

    bronze_df = bronze_df.withColumn("kafka_timestamp", current_timestamp()).withColumn(
        "ingestion_ts", current_timestamp()
    )
    silver_df = silver.transform_to_silver(bronze_df, config).cache()
    kept = silver_df.count()
    bounds = silver_df.agg(spark_min("event_ts"), spark_max("event_ts")).first()
    if not kept or bounds[0] is None:
        print("No events passed the filters; nothing to select.", file=sys.stderr)
        return 1

    start, end = _as_utc(bounds[0]), _as_utc(bounds[1])
    step = timedelta(minutes=args.step_minutes)
    # The last window runs once the newest event has matured, so every event gets a chance
    last = end + timedelta(minutes=config.hold_minutes)
    now = min(start + max(step, timedelta(minutes=config.hold_minutes)), last)
    selected: Dict[str, Dict[str, Any]] = {}
    windows = 0
    while True:
        publishable_df, _ = silver.publishable_events(silver_df, now, config)
        merge_selection(selected, silver.build_topic_records(publishable_df, now, config), now)
        windows += 1
        if now >= last:
            break
        now = min(now + step, last)

    rows = sorted(selected.values(), key=lambda row: row["first_selected_at"])
    write_rows(args.out, rows)
    print(
        f"{kept} events kept between {start:%Y-%m-%d %H:%M} and {end:%H:%M} UTC; "
        f"{windows} windows; {len(rows)} articles selected → {args.out}",
        file=sys.stderr,
    )
    return 0


# ---------------------------------------------------------------------------
# score
# ---------------------------------------------------------------------------


def parse_label(value: Any) -> Optional[bool]:
    normalized = str(value or "").strip().lower()
    if normalized in POSITIVE_LABELS:
        return True
    if normalized in NEGATIVE_LABELS:
        return False
    return None


def precision_report(rows: List[Dict[str, Any]], k: int) -> Dict[str, Any]:
    """Precision over labelled rows, and precision@k when ranked by ``peak_score``."""
    labelled = [row for row in rows if parse_label(row.get("newsworthy")) is not None]
    ranked = sorted(labelled, key=lambda row: float(row.get("peak_score") or 0.0), reverse=True)
    positives = sum(parse_label(row["newsworthy"]) for row in labelled)
    top_k = ranked[:k]
    return {
        "rows": len(rows),
        "labelled": len(labelled),
        "newsworthy": positives,
        "precision": positives / len(labelled) if labelled else None,
        "k": len(top_k),
        "precision_at_k": sum(parse_label(row["newsworthy"]) for row in top_k) / len(top_k) if top_k else None,
    }


def _format(report: Dict[str, Any], title: str) -> str:
    def pct(value: Optional[float]) -> str:
        return "n/a" if value is None else f"{value:.0%}"

    return (
        f"{title}\n"
        f"  articles: {report['rows']}  labelled: {report['labelled']}  newsworthy: {report['newsworthy']}\n"
        f"  precision: {pct(report['precision'])}  precision@{report['k']}: {pct(report['precision_at_k'])}"
    )


def run_score(args: argparse.Namespace) -> int:
    labelled_rows = read_rows(args.labels)
    print(_format(precision_report(labelled_rows, args.k), f"Labels: {args.labels}"))
    if args.candidates:
        # Score another configuration's output with the labels already written for this one
        labels = {row["topic_key"]: row.get("newsworthy", "") for row in labelled_rows}
        other_rows = [
            {**row, "newsworthy": labels.get(row["topic_key"], row.get("newsworthy", ""))}
            for row in read_rows(args.candidates)
        ]
        print(_format(precision_report(other_rows, args.k), f"Candidates: {args.candidates}"))
        unlabelled = sum(1 for row in other_rows if parse_label(row.get("newsworthy")) is None)
        if unlabelled:
            print(f"  ({unlabelled} of its articles have no label yet)")
    return 0


# ---------------------------------------------------------------------------
# CSV + CLI
# ---------------------------------------------------------------------------


def write_rows(path: str, rows: List[Dict[str, Any]]) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    # utf-8-sig so Excel opens accents correctly
    with open(path, "w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def read_rows(path: str) -> List[Dict[str, Any]]:
    with open(path, encoding="utf-8-sig", newline="") as handle:
        sample = handle.read(4096)
        handle.seek(0)
        try:
            # Excel in Spanish locales saves CSV with ";"
            dialect = csv.Sniffer().sniff(sample, delimiters=",;")
        except csv.Error:
            dialect = csv.excel
        return list(csv.DictReader(handle, dialect=dialect))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest="command", required=True)

    select = commands.add_parser("select", help="replay events through the Silver selection")
    source = select.add_mutually_exclusive_group(required=True)
    source.add_argument("--input", help="JSON Lines file recorded with tools/record_stream.py")
    source.add_argument("--bronze", action="store_true", help="read the Bronze Delta table in MinIO")
    select.add_argument("--out", required=True, help="CSV to write")
    select.add_argument("--step-minutes", type=float, default=5, help="window slide (default: 5)")
    select.add_argument("--master", default="local[*]", help="Spark master for file input")
    select.add_argument("--wikis", help="e.g. enwiki,eswiki or '*' (default: SILVER_ALLOWED_WIKIS)")
    select.add_argument("--types", help="e.g. edit,new or '*' (default: SILVER_ALLOWED_TYPES)")
    select.add_argument("--namespaces", help="e.g. 0 or '*' (default: SILVER_ALLOWED_NAMESPACES)")
    select.add_argument("--min-edits", type=int)
    select.add_argument("--min-editors", type=int)
    select.add_argument("--top-n", type=int)
    select.add_argument("--lookback-minutes", type=int)
    select.add_argument("--hold-minutes", type=int)
    select.add_argument("--sample-size", type=int)
    select.set_defaults(handler=run_select)

    score = commands.add_parser("score", help="precision of a labelled CSV")
    score.add_argument("--labels", required=True, help="CSV from 'select' with the newsworthy column filled")
    score.add_argument("--candidates", help="another 'select' CSV to score with the same labels")
    score.add_argument("--k", type=int, default=10, help="cut-off for precision@k (default: 10)")
    score.set_defaults(handler=run_score)
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    return args.handler(args)


if __name__ == "__main__":
    sys.exit(main())
