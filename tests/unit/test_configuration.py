"""Configuration drift: every setting a service reads must be wired through Compose and documented."""

import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
ENV_READ = re.compile(r'(?:os\.getenv|env\.get)\(\s*"([A-Z0-9_]+)"')
INTERPOLATED = re.compile(r"\$\{([A-Z0-9_]+)")

SERVICES = {
    "producer": ["producer/producer.py"],
    "spark-silver": ["spark-jobs/silver_processing.py", "spark-jobs/common/spark_common.py"],
    "spark-gold": [
        "spark-jobs/gold_enrichment.py",
        "spark-jobs/common/spark_common.py",
        "spark-jobs/common/wiki_context.py",
    ],
}


def _compose_environment(service: str) -> dict:
    compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text())
    entries = compose["services"][service].get("environment", {})
    if isinstance(entries, dict):
        return {key: str(value) for key, value in entries.items()}
    return dict(entry.split("=", 1) if "=" in entry else (entry, "") for entry in entries)


def _documented_variables() -> set:
    lines = (ROOT / ".env.example").read_text().splitlines()
    return {line.split("=", 1)[0].strip() for line in lines if "=" in line and not line.lstrip().startswith("#")}


def _read_settings(sources) -> set:
    return {name for source in sources for name in ENV_READ.findall((ROOT / source).read_text())}


@pytest.mark.parametrize("service, sources", SERVICES.items())
def test_every_setting_read_by_a_service_is_passed_by_compose(service, sources):
    read = _read_settings(sources)

    assert read, f"no settings found in {sources}"
    assert read - set(_compose_environment(service)) == set(), "read by the code but not passed by docker-compose.yml"


@pytest.mark.parametrize("service", [*SERVICES, "selection-lab"])
def test_every_configurable_compose_setting_is_documented(service):
    interpolated = {name for value in _compose_environment(service).values() for name in INTERPOLATED.findall(value)}

    assert interpolated, f"{service} exposes no configurable settings"
    assert interpolated - _documented_variables() == set(), "configurable in docker-compose.yml but not in .env.example"


def test_selection_lab_replays_with_the_same_settings_as_silver():
    silver = _compose_environment("spark-silver")
    lab = _compose_environment("selection-lab")
    selection = {key: value for key, value in silver.items() if key.startswith("SILVER_")}
    selection.pop("SILVER_TRIGGER_INTERVAL")

    assert selection
    assert {key: lab.get(key) for key in selection} == selection
