import time
import uuid

from framework.internal.http.mail import MailApi
from framework.internal.http.register import AccountApi
from framework.internal.kafka.producer import Producer
from helpers.mailapi_helper import MailApiHelper


def test_success_registration_via_register_events_errors(kafka_producer: Producer,
                                                         account: AccountApi,
                                                         mail: MailApi,
                                                         mailapi_helper: MailApiHelper):
    base = uuid.uuid4().hex
    message = {
      "input_data": {
        "login": base,
        "email": f"{base}@mail.ru",
        "password": "123123123",
      },
      "error_message": {
        "type": "https://tools.ietf.org/html/rfc7231#section-6.5.1",
        "title": "Validation failed",
        "status": 400,
        "traceId": "00-2bd2ede7c3e4dcf40c4b7a62ac23f448-839ff284720ea656-01",
        "errors": {
          "Email": [
            "Invalid"
          ]
        }
      },
      "error_type": "unknown"
    }

    kafka_producer.send("register-events-errors", message)
    for _ in range(10):
        response = mail.find_message(query=base)
        if response.json()["total"] > 0:
            break
        time.sleep(1)
    else:
        raise AssertionError("Email not found")
    token = mailapi_helper.get_activation_token_by_login(login=base)

    activate_reponse = account.account_token(token=token)
    assert activate_reponse.status_code == 200
    assert activate_reponse.json()["resource"]["login"] == base
