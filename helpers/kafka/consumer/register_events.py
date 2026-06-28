import time
from typing import (
    Optional,
    Any,
)

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


class RegisterEventsErrorsSubscriber(Subscriber):
    """Подписчик на топик с ошибками регистрации"""
    topic: str = "register-events-errors"

    def __init__(self):
        super().__init__()

    def find_error_message(
            self, login: str, error_type: str = "validation", timeout: float = 10.0
    ) -> None:
        start_time = time.time()

        while time.time() - start_time < timeout:
            try:
                message = self.get_message(timeout=1.0)
                input_data = message.value.get("input_data", {})
                message_login = input_data.get("login")
                message_error_type = message.value.get("error_type")
                if (message_login == login
                        and message_error_type == error_type):
                    return message
            except AssertionError:
                continue
            except Exception as e:
                print(f"Unexpected error while getting message: {e}")
                continue

        raise AssertionError(
            f"Error message not found for login '{login}' in topic {self.topic} "
            f"with error_type '{error_type}' within timeout {timeout}s"
        )
