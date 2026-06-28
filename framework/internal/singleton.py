import threading
from typing import (
    Optional,
    Any,
)


class Singleton:
    _instance: Any | None = None
    _lock: threading.Lock = threading.Lock()

    def __new__(cls, *args, **kwargs) -> 'Singleton':
        if cls._instance is None:
            with cls._lock:
                # Двойная проверка блокировки (Double-Checked Locking)
                if cls._instance is None:
                    cls._instance = super().__new__(cls, *args, **kwargs)
        return cls._instance
