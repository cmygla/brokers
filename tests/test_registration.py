import time
import uuid

from framework.internal.kafka.producer import Producer
from framework.internal.rmq.publisher import RmqPublisher
from helpers.kafka.consumer.register_events import RegisterEventsSubscriber
from framework.internal.http.mail import MailApi
from framework.internal.http.register import AccountApi
from helpers.mailapi_helper import MailApiHelper

from helpers.rmq.consumer.dm_mail_sending import DmMailSending


def test_success_registration(
        register_message, account: AccountApi, mailapi_helper: MailApiHelper, ) -> None:
    """
    Тест успешной регистрации с проверкой почты
    """
    message = register_message()
    login = message["login"]
    account.register_user(**message)
    mailapi_helper.find_email(login)


def test_success_registration_with_kafka_producer(
        register_message, kafka_producer: Producer, mailapi_helper: MailApiHelper, ) -> None:
    """
    Тест регистрации с отправкой события через Kafka Producer
    """
    message = register_message()
    login = message["login"]
    kafka_producer.send(topic="register-events", message=message)
    mailapi_helper.find_email(login)


def test_success_registration_with_kafka_producer_consumer(
        register_message,
        register_events_subscriber: RegisterEventsSubscriber,
        kafka_producer: Producer,
        mailapi_helper: MailApiHelper,
) -> None:
    """
    Тест регистрации с использованием Kafka Producer и Consumer
    """
    message = register_message()
    login = message["login"]
    kafka_producer.send(topic="register-events", message=message)
    register_events_subscriber.find_message(login=login)
    mailapi_helper.find_email(login)


def test_success_e2e_registration(
        rmq_dm_mail_sending_consumer: DmMailSending,
        register_events_subscriber: RegisterEventsSubscriber,
        register_message,
        account: AccountApi,
        mailapi_helper: MailApiHelper, ) -> None:
    message = register_message()
    login = message["login"]
    account.register_user(**message)
    register_events_subscriber.find_message(login=login)
    rmq_dm_mail_sending_consumer.find_message(login=login)
    mailapi_helper.find_email(login)


def test_rmq_publisher(rmq_publisher: RmqPublisher, mailapi_helper: MailApiHelper, rmq_message) -> None:
    login = uuid.uuid4().hex
    message = rmq_message(login)
    rmq_publisher.publish(exchange="dm.mail.sending", message=message)
    mailapi_helper.find_email(login)
