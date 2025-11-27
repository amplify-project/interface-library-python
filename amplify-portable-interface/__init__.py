from connection import Connection, ConnectionType


def connect(host: str, port: int, type: ConnectionType) -> Connection:
    return Connection(host, port, type)
