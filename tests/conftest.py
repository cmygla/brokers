import pytest
import uuid

from framework.internal.kafka.consumer import Consumer
from framework.internal.kafka.producer import Producer
from framework.internal.http.mail import MailApi
from framework.internal.http.register import AccountApi
from helpers.kafka.consumer.register_events import RegisterEventsSubscriber
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
def register_message() -> dict[str, str]:
    """Фикстура с данными для регистрации"""
    base = uuid.uuid4().hex
    return {
        "login": base,
        "email": f"{base}@mail.ru",
        "password": "123123123", }


@pytest.fixture
def register_error_message() -> dict[str, str]:
    """Фикстура с данными для регистрации"""
    base = uuid.uuid4().hex
    return {
        "input_data": {
            "login": base,
            "email": f"{base}@mail.ru",
            "password": "123123123", },
        "error_message": {
            "type": "https://tools.ietf.org/html/rfc7231#section-6.5.1",
            "title": "Validation failed",
            "status": 400,
            "traceId": "00-2bd2ede7c3e4dcf40c4b7a62ac23f448-839ff284720ea656-01",
            "errors": {
                "Email": ["Invalid"]}},
        "error_type": "unknown"}


@pytest.fixture(scope="session")
def register_events_subscriber() -> RegisterEventsSubscriber:
    """Фикстура подписчика на события регистрации"""
    return RegisterEventsSubscriber()


@pytest.fixture(scope="session", autouse=True)
def kafka_consumer(
        register_events_subscriber: RegisterEventsSubscriber, ) -> Consumer:
    """Фикстура потребителя Kafka"""
    with Consumer(subscribers=[register_events_subscriber]) as consumer:
        yield consumer


@pytest.fixture(scope="session")
def kafka_producer() -> Producer:
    with Producer() as producer:
        yield producer