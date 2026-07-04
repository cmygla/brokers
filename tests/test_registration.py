import time
import uuid

from framework.internal.kafka.producer import Producer
from framework.internal.rmq.publisher import RmqPublisher
from helpers.kafka.consumer.register_events import RegisterEventsSubscriber
from framework.internal.http.mail import MailApi
from framework.internal.http.register import AccountApi

from helpers.rmq.consumer.dm_mail_sending import DmMailSending


def test_success_registration(
        register_message, account: AccountApi, mail: MailApi, ) -> None:
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
        register_message, kafka_producer: Producer, mail: MailApi, ) -> None:
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
        register_message, register_events_subscriber: RegisterEventsSubscriber, kafka_producer: Producer, mail: MailApi
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





def test_success_e2e_registration(
        rmq_dm_mail_sending_consumer: DmMailSending,
        register_events_subscriber: RegisterEventsSubscriber,
        register_message,
        account: AccountApi,
        mail: MailApi, ) -> None:
    """
    Полный E2E тест регистрации:
    1. Регистрация пользователя
    2. Проверка события в Kafka (RegisterEvents)
    3. Проверка сообщения в RabbitMQ (dm.mail.sending)
    4. Проверка письма в почтовом ящике
    """
    message = register_message()
    login = message["login"]

    # 1. Регистрация пользователя
    account.register_user(**message)

    # 2. Проверка события в Kafka
    register_events_subscriber.find_message(login=login)

    # 3. Проверка сообщения в RabbitMQ
    rmq_dm_mail_sending_consumer.find_message(login=login)

    # 4. Проверка письма в почте (с повторными попытками)
    for _ in range(10):
        response = mail.find_message(query=login)
        if response.json()["total"] > 0:
            break
        time.sleep(1)
    else:
        raise AssertionError("Email not found")


def test_rmq(rmq_publisher: RmqPublisher) -> None:
    address = f"{uuid.uuid4().hex}@mail.ru"
    message = {
        "address": address,
        "subject": "Published message",
        "body": "Published message", }
    rmq_publisher.publish(exchange="dm.mail.sending", message=message)
