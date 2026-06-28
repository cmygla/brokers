import queue
import threading
import time
from typing import Any

from framework.internal.kafka.subscriber import Subscriber


class RegisterEventsSubscriber(Subscriber):
    """Подписчик на события регистрации"""
    topic: str = "register-events"

    def __init__(self):
        super().__init__()

    def find_message(self, login: str, timeout: float = 10.0) -> None:
        """
        Поиск сообщения по логину в течение указанного времени
        """
        start_time = time.time()

        while time.time() - start_time < timeout:
            try:
                message = self.get_message(timeout=1.0)
                if message.value.get("login") == login:
                    return
            except AssertionError:
                # Если сообщений нет, продолжаем ждать
                continue

        raise AssertionError(f"Message with login '{login}' not found in topic {self.topic}")
