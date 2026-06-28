import pytest

from framework.internal.http.mail import MailApi
from framework.internal.http.register import AccountApi
from framework.internal.kafka.producer import Producer
from helpers.mailapi_helper import MailApiHelper


@pytest.fixture(scope="session")
def account() -> AccountApi:
    return AccountApi()


@pytest.fixture(scope="session")
def mail() -> MailApi:
    return MailApi()


@pytest.fixture(scope="session")
def kafka_producer() -> Producer:
    with Producer() as producer:
        yield producer


@pytest.fixture()
def mailapi_helper(mail) -> MailApiHelper:
    return MailApiHelper(mailapi_client=mail)
