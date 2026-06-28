import pytest
import uuid

from framework.internal.kafka.consumer import Consumer
from framework.internal.kafka.producer import Producer
from framework.internal.http.mail import MailApi
from framework.internal.http.register import AccountApi
from helpers.kafka.consumer.register_events import (
    RegisterEventsSubscriber,
    RegisterEventsErrorsSubscriber,
)
from helpers.mailapi_helper import MailApiHelper


@pytest.fixture(scope="session")
def account() -> AccountApi:
    return AccountApi()


@pytest.fixture(scope="session")
def mail() -> MailApi:
    return MailApi()


@pytest.fixture()
def mailapi_helper(mail) -> MailApiHelper:
    return MailApiHelper(mailapi_client=mail)


@pytest.fixture
def register_message():
    def _register_message(login: str = None, password: str = "123123123"):
        """Фикстура с данными для регистрации"""
        if login is None:
            login = uuid.uuid4().hex
        return {
            "login": login,
            "email": f"{login}@mail.ru",
            "password": password }
    return _register_message


@pytest.fixture
def invalid_login_data() -> dict[str, str]:
    invalid_login = "string"
    email = f"{invalid_login}@mail.ru"
    invalid_password = "string"

    invalid_data = {
        "login": invalid_login,
        "email": email,
        "password": invalid_password, }
    return invalid_data


@pytest.fixture
def register_error_message():
    def _register_error_message(login: str = None, password: str = "123123123"):
        """Фикстура с данными для регистрации"""
        if login is None:
            login = uuid.uuid4().hex
        return {
            "input_data": {
                "login": login,
                "email": f"{login}@mail.ru",
                "password": password, },
            "error_message": {
                "type": "https://tools.ietf.org/html/rfc7231#section-6.5.1",
                "title": "Validation failed",
                "status": 400,
                "traceId": "00-2bd2ede7c3e4dcf40c4b7a62ac23f448-839ff284720ea656-01",
                "errors": {
                    "Email": ["Invalid"]}},
            "error_type": "unknown"}
    return _register_error_message


@pytest.fixture(scope="session")
def register_events_subscriber() -> RegisterEventsSubscriber:
    """Подписчик на успешные события регистрации"""
    return RegisterEventsSubscriber()


@pytest.fixture(scope="session")
def register_events_errors_subscriber() -> RegisterEventsErrorsSubscriber:
    """Подписчик на ошибки регистрации"""
    return RegisterEventsErrorsSubscriber()


@pytest.fixture(scope="session", autouse=True)
def kafka_consumer(
    register_events_subscriber: RegisterEventsSubscriber,
    register_events_errors_subscriber: RegisterEventsErrorsSubscriber,
) -> Consumer:
    """Фикстура потребителя Kafka с двумя подписчиками"""
    with Consumer(
        subscribers=[
            register_events_subscriber,
            register_events_errors_subscriber,
        ]
    ) as consumer:
        yield consumer


@pytest.fixture(scope="session")
def kafka_producer() -> Producer:
    with Producer() as producer:
        yield producer