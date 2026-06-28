import time

from framework.internal.kafka.producer import Producer
from helpers.kafka.consumer.register_events import RegisterEventsSubscriber
from framework.internal.http.mail import MailApi
from framework.internal.http.register import AccountApi


def test_failed_registration(account: AccountApi, mail: MailApi) -> None:
    expected_mail = "string1@mail.ru"
    account.register_user(login="string", email=expected_mail, password="string")
    for _ in range(10):
        response = mail.find_message(query=expected_mail)
        if response.json()["total"] > 0:
            raise AssertionError("Email not found")
        time.sleep(1)


def test_success_registration(
        register_events_subscriber: RegisterEventsSubscriber,
        register_message: dict[str, str],
        account: AccountApi,
        mail: MailApi, ) -> None:
    """
    Тест успешной регистрации с проверкой почты
    """
    login = register_message["login"]
    account.register_user(**register_message)
    for _ in range(10):
        response = mail.find_message(query=login)
        if response.json()["total"] > 0:
            break
        time.sleep(1)
    else:
        raise AssertionError("Email not found")


def test_success_registration_with_kafka_producer(
        register_message: dict[str, str], kafka_producer: Producer, mail: MailApi,
) -> None:
    """
    Тест регистрации с отправкой события через Kafka Producer
    """
    login = register_message["login"]
    kafka_producer.send(topic="register-events", message=register_message)

    for _ in range(10):
        response = mail.find_message(query=login)
        if response.json()["total"] > 0:
            break
        time.sleep(1)
    else:
        raise AssertionError("Email not found")


def test_success_registration_with_kafka_producer_consumer(
        register_message: dict[str, str],
        register_events_subscriber: RegisterEventsSubscriber,
        kafka_producer: Producer, ) -> None:
    """
    Тест регистрации с использованием Kafka Producer и Consumer
    """
    login = register_message["login"]
    kafka_producer.send(topic="register-events", message=register_message)
    register_events_subscriber.find_message(login=login)
