import json
import logging
import os
import signal
import sys
import time
from typing import Any

from confluent_kafka import Producer
from requests_sse import EventSource


logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("wiki-producer")


SSE_URL = os.getenv("WIKI_SSE_URL", "https://stream.wikimedia.org/v2/stream/recentchange")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("REDPANDA_BROKER", "redpanda:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_RAW", "wiki-raw")
USER_AGENT = os.getenv(
    "WIKI_USER_AGENT",
    "wikipedia-producer/1.0 (https://wikipedia.local; classroom@local)",
)
RECONNECT_DELAY_SECONDS = float(os.getenv("PRODUCER_RECONNECT_DELAY", "5"))
LOG_EVERY_N_MESSAGES = int(os.getenv("PRODUCER_LOG_EVERY_N", "100"))


class GracefulStop:
    def __init__(self) -> None:
        self.stop = False
        signal.signal(signal.SIGINT, self._handle)
        signal.signal(signal.SIGTERM, self._handle)

    def _handle(self, signum: int, _frame: Any) -> None:
        logger.info("Signal %s received, stopping producer...", signum)
        self.stop = True


def delivery_report(err, msg) -> None:
    if err is not None:
        logger.error("Kafka delivery failed: %s", err)


def is_valid_change(change: dict[str, Any]) -> bool:
    if not isinstance(change, dict):
        return False
    if "meta" not in change or not isinstance(change["meta"], dict):
        return False
    if "id" not in change:
        return False
    if "title" not in change:
        return False
    return True


def build_producer() -> Producer:
    return Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "client.id": "wiki-sse-producer",
            "acks": "all",
            "retries": 10,
            "request.timeout.ms": 10000,
            "message.timeout.ms": 30000,
            "enable.idempotence": True,
        }
    )


def run() -> None:
    controller = GracefulStop()
    producer = build_producer()

    total_sent = 0
    total_errors = 0
    last_log_at = time.time()

    logger.info("Starting producer. SSE=%s topic=%s broker=%s", SSE_URL, KAFKA_TOPIC, KAFKA_BOOTSTRAP_SERVERS)

    while not controller.stop:
        try:
            with EventSource(SSE_URL, headers={"User-Agent": USER_AGENT}) as stream:
                logger.info("Connected to Wikipedia SSE stream")
                for event in stream:
                    if controller.stop:
                        break

                    if event.type != "message" or not event.data:
                        continue

                    try:
                        payload = json.loads(event.data)
                    except json.JSONDecodeError:
                        total_errors += 1
                        logger.warning("Invalid JSON event discarded")
                        continue

                    if not is_valid_change(payload):
                        total_errors += 1
                        logger.debug("Event failed structural validation and was discarded")
                        continue

                    if payload.get("meta", {}).get("domain") == "canary":
                        continue

                    payload_bytes = json.dumps(payload, ensure_ascii=False).encode("utf-8")

                    try:
                        producer.produce(KAFKA_TOPIC, payload_bytes, callback=delivery_report)
                        producer.poll(0)
                        total_sent += 1
                    except BufferError:
                        total_errors += 1
                        producer.poll(1)
                        logger.warning("Producer queue is full, retrying...")
                        continue

                    if total_sent % LOG_EVERY_N_MESSAGES == 0:
                        now = time.time()
                        elapsed = max(now - last_log_at, 1e-6)
                        rate = LOG_EVERY_N_MESSAGES / elapsed
                        logger.info(
                            "sent=%d errors=%d rate=%.2f events/s",
                            total_sent,
                            total_errors,
                            rate,
                        )
                        last_log_at = now

        except Exception as exc:
            total_errors += 1
            logger.error("SSE connection error: %s", exc)
            if controller.stop:
                break
            logger.info("Reconnecting in %.1f seconds...", RECONNECT_DELAY_SECONDS)
            time.sleep(RECONNECT_DELAY_SECONDS)

    logger.info("Flushing Kafka producer before exit...")
    producer.flush(10)
    logger.info("Producer stopped. sent=%d errors=%d", total_sent, total_errors)


if __name__ == "__main__":
    try:
        run()
    except Exception as exc:
        logger.exception("Fatal error in producer: %s", exc)
        sys.exit(1)
