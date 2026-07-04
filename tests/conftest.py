from typing import Generator

import pytest
import uuid

from framework.internal.kafka.consumer import Consumer
from framework.internal.kafka.producer import Producer
from framework.internal.http.mail import MailApi
from framework.internal.http.register import AccountApi
from framework.internal.rmq.publisher import RmqPublisher
from helpers.kafka.consumer.register_events import (
    RegisterEventsSubscriber,
    RegisterEventsErrorsSubscriber,
)
from helpers.mailapi_helper import MailApiHelper

from helpers.rmq.consumer.dm_mail_sending import DmMailSending


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
def rmq_message():
    def _rmq_message(login: str = None, password: str = "123123123"):
        if login is None:
            login = uuid.uuid4().hex
        address = f"{login}@mail.ru"
        return {
                "address": address,
                "subject": "Published message",
                "body": "Published message", }
    return _rmq_message


@pytest.fixture
def register_message():
    def _register_message(login: str = None, password: str = "123123123"):
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
def kafka_producer() -> Generator[Producer, None, None]:
    """Фикстура для Kafka Producer."""
    with Producer() as producer:
        yield producer


@pytest.fixture(scope="session")
def rmq_publisher() -> Generator[RmqPublisher, None, None]:
    """Фикстура для RabbitMQ Publisher."""
    with RmqPublisher() as publisher:
        yield publisher


@pytest.fixture(scope="session", autouse=True)
def rmq_dm_mail_sending_consumer() -> Generator[DmMailSending, None, None]:
    """
    Фикстура для RabbitMQ Consumer (dm.mail.sending).
    Автоматически подключается для всех тестов.
    """
    with DmMailSending() as consumer:
        yield consumer