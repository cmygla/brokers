import json
import threading
from types import TracebackType
from typing import Optional, \
    Type
from kafka import KafkaProducer
from kafka.producer.future import RecordMetadata

from framework.internal.singleton import Singleton


class Producer(Singleton):
    def __init__(self, bootstrap_servers: list[str]=None):
        if bootstrap_servers is None:
            bootstrap_servers = ["185.185.143.231:9092"]
        self._bootstrap_servers = bootstrap_servers
        self._producer: Optional[KafkaProducer] = None
        self._lock = threading.Lock()

    def start(self) -> None:
        self._producer = KafkaProducer(
            bootstrap_servers=self._bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
            acks="all",
            retries=5,
            retry_backoff_ms=5000,
            request_timeout_ms=70000,
            connections_max_idle_ms=65000,
            reconnect_backoff_ms=5000,
            reconnect_backoff_max_ms=10000, )

    def stop(self) -> None:
        if self._producer:
            self._producer.close()
            self._producer = None

    def send(self, topic: str, message: dict[str, str], timeout: float = 60.0) -> RecordMetadata:
        if not self._producer:
            raise RuntimeError("Producer is not started")
        try:
            with self._lock:
                future = self._producer.send(topic, value=message)
                # Ожидание подтверждения с указанным таймаутом
                record_metadata = future.get(timeout=timeout)
                return record_metadata
        except Exception as e:
            raise RuntimeError(f"Failed to send message to Kafka: {e}")

    def __enter__(self) -> "Producer":
        self.start()
        return self

    def __exit__(
            self,
            exc_type: Optional[Type[BaseException]],
            exc_val: Optional[BaseException],
            exc_tb: Optional[TracebackType], ) -> None:
        self.stop()

