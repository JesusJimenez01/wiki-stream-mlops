"""Shared pytest setup: make every service and tool importable straight from the repository."""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

for service_dir in ("spark-jobs", "producer", "newsroom-api", "tools"):
    path = str(ROOT / service_dir)
    if path not in sys.path:
        sys.path.insert(0, path)
