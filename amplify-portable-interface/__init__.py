from .connection import Connection, ConnectionType
from typing import Any


def connect(host: str, type: ConnectionType, device_info: Any = None) -> Connection:
    return Connection(host, type, device_info)
