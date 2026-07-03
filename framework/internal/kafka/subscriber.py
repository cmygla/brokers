from abc import (
    ABC,
    abstractmethod,
)
from typing import Any
import queue

from kafka.consumer.fetcher import ConsumerRecord


class Subscriber(ABC):
    """Базовый класс для подписчиков на Kafka"""
    def __init__(self):
        self._messages: queue.Queue = queue.Queue()

    @property
    @abstractmethod
    def topic(self) -> str: ...

    def handle_message(self, record: ConsumerRecord) -> None:
        self._messages.put(record)

    def get_message(self, timeout: float = 10.0) -> Any:
        """Получение сообщения из очереди"""
        try:
            return self._messages.get(timeout=timeout)
        except BaseException:
            raise AssertionError(
                f"No messages from topic: {self.topic}, within timeout {timeout}"
            )