import time

import json_stream
from framework.internal.rmq.consumer import Consumer


def _contains_login(message, login: str) -> bool:
    if isinstance(message.get('body'), str):
        try:
            body_data = json_stream.load(message['body'], persistent=True)
            return body_data.get('Login') == login
        except BaseException:
            pass
    elif isinstance(message.get('body'), dict):
        return message['body'].get('Login') == login
    return False


class DmMailSending(Consumer):
    exchange = "dm.mail.sending"
    routing_key = "#"

    def find_message(self, login: str, timeout: float = 10.0) -> None:
        start_time = time.time()
        while time.time() - start_time < timeout:
            message = self.get_message(timeout=timeout)
            if _contains_login(message, login):
                break
        else:
            raise AssertionError(f"Message with login '{login}' not found")
