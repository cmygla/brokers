import time
import uuid

from framework.internal.http.mail import MailApi
from framework.internal.http.register import AccountApi
from framework.internal.kafka.producer import Producer
from helpers.kafka.consumer.register_events import (
    RegisterEventsErrorsSubscriber,
    RegisterEventsSubscriber,
)
from helpers.mailapi_helper import MailApiHelper


def test_failed_registration(account: AccountApi, mail: MailApi, invalid_login_data: dict[str, str]) -> None:
    account.register_user(**invalid_login_data)
    for _ in range(10):
        response = mail.find_message(query=invalid_login_data["email"])
        if response.json()["total"] > 0:
            raise AssertionError("Email not found")
        time.sleep(1)


def test_failed_registration_consumer(
        register_events_subscriber: RegisterEventsSubscriber,
        register_events_errors_subscriber: RegisterEventsErrorsSubscriber,
        account: AccountApi,
        invalid_login_data: dict[str, str]
) -> None:
    account.register_user(**invalid_login_data)

    register_events_subscriber.find_message(login=invalid_login_data["login"], timeout=10.0)

    register_events_errors_subscriber.find_error_message(
        login=invalid_login_data["login"], error_type="validation", timeout=10.0
    )


def test_success_registration_via_register_events_errors(
        kafka_producer: Producer,
        account: AccountApi,
        mail: MailApi,
        mailapi_helper: MailApiHelper,
        register_error_message
        ):
    message = register_error_message()
    login = message["input_data"]["login"]

    kafka_producer.send("register-events-errors", message)
    for _ in range(10):
        response = mail.find_message(query=login)
        if response.json()["total"] > 0:
            break
        time.sleep(1)
    else:
        raise AssertionError("Email not found")
    token = mailapi_helper.get_activation_token_by_login(login=login)

    activate_response = account.account_token(token=token)
    assert activate_response.status_code == 200
    assert activate_response.json()["resource"]["login"] == login


def test_unknown_error_type_retry_to_validation(
        kafka_producer: Producer,
        register_events_errors_subscriber: RegisterEventsErrorsSubscriber,
        register_error_message,
        invalid_login_data: dict[str, str]
) -> None:
    message = register_error_message(invalid_login_data["login"], invalid_login_data["password"])
    login = message["input_data"]["login"]

    kafka_producer.send("register-events-errors", message)
    register_events_errors_subscriber.find_error_message(
        login=login, error_type="validation", timeout=10.0
    )
