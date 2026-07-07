import json
import queue
import threading
import time

import pika
from framework.internal.singleton import Singleton


class Consumer(Singleton):
    """
    Базовый класс для RabbitMQ Consumer.
    Создает временную очередь, подписывается на exchange/routing_key.
    """
    _started: bool = False

    def __init__(self, url: str = "amqp://guest:guest@185.185.143.231:5672"):
        self._url = url
        self._connection = pika.BlockingConnection(pika.URLParameters(self._url))
        self._channel = self._connection.channel()
        self._running = threading.Event()
        self._ready = threading.Event()
        self._thread: threading.Thread | None = None
        self._messages: queue.Queue = queue.Queue()
        self._queue_name: str = ""

    @property
    def exchange(self):
        """Exchange для подписки. Должен быть переопределен в наследнике."""
        raise NotImplementedError("Set exchange")

    @property
    def routing_key(self):
        """Routing key для подписки. Должен быть переопределен в наследнике."""
        raise NotImplementedError("Set routing_key")

    def _start(self):
        """
        Запускает consumer:
        - объявляет временную очередь
        - биндит её на exchange/routing_key
        - запускает поток прослушивания
        """
        result = self._channel.queue_declare(
            queue="", exclusive=True, auto_delete=True, durable=False, )
        self._queue_name = result.method.queue
        print(f"Declare queue with name {self._queue_name}")

        self._channel.queue_bind(
            queue=self._queue_name, exchange=self.exchange, routing_key=self.routing_key, )

        self._running.set()
        self._ready.clear()
        self._thread = threading.Thread(target=self._consume, daemon=True)
        self._thread.start()

        if not self._ready.wait(timeout=10):
            raise RuntimeError("Consumer is not ready")

        self._started = True

    def _consume(self):
        """
        Основной цикл потребления сообщений.
        Работает в отдельном потоке.
        """
        self._ready.set()
        print("Consumer rmq started")

        def on_message_callback(ch, method, properties, body):
            """Callback при получении сообщения."""
            try:
                body_str = body.decode("utf-8")
                try:
                    data = json.loads(body_str)
                except json.JSONDecodeError:
                    data = body_str
                self._messages.put(data)
                print("Received message", data)
            except Exception as e:
                print(f"Error while processing message rmq: {e}")

        self._channel.basic_consume(
            self._queue_name, on_message_callback, auto_ack=True
        )

        try:
            while self._running.is_set():
                self._connection.process_data_events(time_limit=0.1)
                time.sleep(0.01)
        except Exception as e:
            print(f"Error: {e}")

    def _stop(self):
        """
        Останавливает consumer:
        - останавливает поток
        - закрывает канал
        - закрывает соединение
        """
        self._running.clear()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2)
            if self._thread.is_alive():
                print("Thread is still alive")

        if self._channel:
            try:
                self._channel.close()
                print("Close channel")
            except Exception as e:
                print(f"Error while closing consumer: {e}")

        if self._connection:
            try:
                self._connection.close()
                print("Close connection")
            except Exception as e:
                print(f"Error while closing consumer: {e}")

        self._started = False
        print("Consumer stopped")

    def get_message(self, timeout: float = 90):
        try:
            return self._messages.get(timeout=timeout)
        except queue.Empty:
            raise AssertionError(
                f"No messages from Topic: {self._queue_name}, within timeout: {timeout}"
            )

    def __enter__(self):
        self._start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._stop()
