"""Configuration drift: every setting a service reads must be wired through Compose and documented."""

import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
ENV_READ = re.compile(r'os\.getenv\(\s*"([A-Z0-9_]+)"')


def _compose_environment(service: str) -> set:
    compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text())
    entries = compose["services"][service].get("environment", [])
    return {entry.split("=", 1)[0] for entry in entries}


def _documented_variables() -> set:
    lines = (ROOT / ".env.example").read_text().splitlines()
    return {line.split("=", 1)[0].strip() for line in lines if "=" in line and not line.lstrip().startswith("#")}


@pytest.mark.parametrize("service, source", [("producer", "producer/producer.py")])
def test_service_settings_are_wired_and_documented(service, source):
    read = set(ENV_READ.findall((ROOT / source).read_text()))

    assert read, f"no settings found in {source}"
    assert read - _compose_environment(service) == set(), "read by the code but not passed by docker-compose.yml"
    assert read - _documented_variables() == set(), "read by the code but missing from .env.example"
