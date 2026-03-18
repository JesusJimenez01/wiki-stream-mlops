#!/bin/bash
# ============================================================
# Wikipedia — Services Verification (Phase 0)
# ============================================================
# Usage: bash check_services.sh
# Checks that all containers are healthy and running.
# ============================================================

set -e

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

PASS=0
FAIL=0

check() {
    local name="$1"
    local cmd="$2"

    printf "  %-25s " "$name"
    if eval "$cmd" > /dev/null 2>&1; then
        echo -e "[${GREEN}OK${NC}]"
        PASS=$((PASS + 1))
    else
        echo -e "[${RED}FAIL${NC}]"
        FAIL=$((FAIL + 1))
    fi
}

echo ""
echo -e "${YELLOW}========================================${NC}"
echo -e "${YELLOW} Wikipedia — Health Check${NC}"
echo -e "${YELLOW}========================================${NC}"
echo ""

# --- Redpanda ---
check "Redpanda (Kafka API)" \
    "docker exec wiki-redpanda rpk cluster health"

check "Redpanda (topic create)" \
    "docker exec wiki-redpanda rpk topic create wiki-raw --partitions 3 2>/dev/null || docker exec wiki-redpanda rpk topic describe wiki-raw"

# --- Redpanda Console ---
check "Redpanda Console (UI)" \
    "curl -sf http://localhost:8888 > /dev/null"

# --- Spark Master ---
check "Spark Master (Web UI)" \
    "curl -sf http://localhost:8080 > /dev/null"

# --- Spark Worker ---
check "Spark Worker (Web UI)" \
    "curl -sf http://localhost:8081 > /dev/null"

# --- MinIO ---
check "MinIO (API S3)" \
    "curl -sf http://localhost:9000/minio/health/live > /dev/null"

check "MinIO Console (Web UI)" \
    "curl -sf http://localhost:9001 > /dev/null"

# --- MongoDB ---
check "MongoDB (ping)" \
    "docker exec wiki-mongodb mongosh --quiet --eval 'db.adminCommand(\"ping\")' -u wikipedia -p wikipedia123 --authenticationDatabase admin"

# --- Ollama ---
check "Ollama (API)" \
    "docker exec wiki-ollama ollama list"

# --- Producer (Phase 1) ---
check "Producer (container)" \
    "docker inspect -f '{{.State.Running}}' wiki-producer | grep -q true"

# --- Spark Jobs (Phases 2-5) ---
check "Spark Bronze (container)" \
    "docker inspect -f '{{.State.Running}}' wiki-spark-bronze | grep -q true"

check "Spark Silver (container)" \
    "docker inspect -f '{{.State.Running}}' wiki-spark-silver | grep -q true"

check "Spark Gold (container)" \
    "docker inspect -f '{{.State.Running}}' wiki-spark-gold | grep -q true"

check "Spark Serving (container)" \
    "docker inspect -f '{{.State.Running}}' wiki-spark-serving | grep -q true"

# --- Phase 6 ---
check "Newsroom Web/API" \
    "curl -sf http://localhost:8085/health > /dev/null"

check "Prometheus" \
    "curl -sf http://localhost:9090/-/healthy > /dev/null"

check "Grafana" \
    "curl -sf http://localhost:3000/api/health > /dev/null"

check "Node Exporter" \
    "curl -sf http://localhost:9100/metrics > /dev/null"

check "cAdvisor" \
    "curl -sf http://localhost:8088/metrics > /dev/null"

check "DCGM Exporter" \
    "curl -sf http://localhost:9400/metrics > /dev/null"

# --- Summary ---
echo ""
echo -e "${YELLOW}========================================${NC}"
TOTAL=$((PASS + FAIL))
echo -e " Result: ${GREEN}$PASS${NC}/$TOTAL passed"
if [ $FAIL -gt 0 ]; then
    echo -e " ${RED}$FAIL service(s) failed.${NC}"
    echo ""
    exit 1
else
    echo -e " ${GREEN}All services operational.${NC}"
    echo ""
    exit 0
fi
