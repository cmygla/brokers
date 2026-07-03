import time

from framework.internal.kafka.producer import Producer
from helpers.kafka.consumer.register_events import RegisterEventsSubscriber
from framework.internal.http.mail import MailApi
from framework.internal.http.register import AccountApi


def test_success_registration(
        register_message,
        account: AccountApi,
        mail: MailApi, ) -> None:
    """
    Тест успешной регистрации с проверкой почты
    """
    message = register_message()
    login = message["login"]
    account.register_user(**message)
    for _ in range(10):
        response = mail.find_message(query=login)
        if response.json()["total"] > 0:
            break
        time.sleep(1)
    else:
        raise AssertionError("Email not found")


def test_success_registration_with_kafka_producer(
        register_message,
        kafka_producer: Producer,
        mail: MailApi, ) -> None:
    """
    Тест регистрации с отправкой события через Kafka Producer
    """
    message = register_message()
    login = message["login"]
    kafka_producer.send(topic="register-events", message=message)

    for _ in range(10):
        response = mail.find_message(query=login)
        if response.json()["total"] > 0:
            break
        time.sleep(1)
    else:
        raise AssertionError("Email not found")


def test_success_registration_with_kafka_producer_consumer(
        register_message,
        register_events_subscriber: RegisterEventsSubscriber,
        kafka_producer: Producer,
        mail: MailApi
) -> None:
    """
    Тест регистрации с использованием Kafka Producer и Consumer
    """
    message = register_message()
    login = message["login"]
    kafka_producer.send(topic="register-events", message=message)
    register_events_subscriber.find_message(login=login)

    for _ in range(10):
        response = mail.find_message(query=login)
        if response.json()["total"] > 0:
            break
        time.sleep(1)
    else:
        raise AssertionError("Email not found")
