import time
import uuid

from framework.internal.http.mail import MailApi
from framework.internal.http.register import AccountApi
from framework.internal.kafka.producer import Producer
from helpers.mailapi_helper import MailApiHelper


def test_success_registration_via_register_events_errors(kafka_producer: Producer,
                                                         account: AccountApi,
                                                         mail: MailApi,
                                                         mailapi_helper: MailApiHelper,
                                                         register_error_message: dict[str, str]):
    message = register_error_message
    login = message["login"]

    kafka_producer.send("register-events-errors", message)
    for _ in range(10):
        response = mail.find_message(query=login)
        if response.json()["total"] > 0:
            break
        time.sleep(1)
    else:
        raise AssertionError("Email not found")
    token = mailapi_helper.get_activation_token_by_login(login=login)

    activate_reponse = account.account_token(token=token)
    assert activate_reponse.status_code == 200
    assert activate_reponse.json()["resource"]["login"] == login
