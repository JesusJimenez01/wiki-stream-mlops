#!/bin/bash
set -euo pipefail

echo "== Wikipedia basic tests =="

echo "[1/2] Silver data quality"
docker exec wiki-spark-silver /opt/spark/bin/spark-submit --master local[1] /opt/tests/quality_check.py > /tmp/silver_quality.log 2>&1
grep -E "BRONZE_TOTAL=|SILVER_TOTAL=|SILVER_BAD_|QUALITY_STATUS=" /tmp/silver_quality.log || true
grep -q "QUALITY_STATUS=PASS" /tmp/silver_quality.log

echo "[2/2] Gold AI quality"
docker exec wiki-spark-gold /opt/spark/bin/spark-submit --master local[1] /opt/tests/gold_quality_report.py > /tmp/gold_quality.log 2>&1
grep -E "TOTAL_NEWS=|SUCCESS_RATE=|EMPTY_HEADLINE=|EMPTY_SUMMARY=|GOLD_QUALITY_STATUS=" /tmp/gold_quality.log || true
grep -q "GOLD_QUALITY_STATUS=PASS" /tmp/gold_quality.log

echo "All basic tests passed"
