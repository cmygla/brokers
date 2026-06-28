import threading
from typing import (
    Optional,
    Any,
)


class Singleton:
    """Базовый класс для реализации паттерна Singleton"""
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs) -> 'Singleton':
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
