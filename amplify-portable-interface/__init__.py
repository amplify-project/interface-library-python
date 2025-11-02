from connection import Connection


def connect(host: str, port: int, type: str) -> Connection:
    return Connection(host, port, type)
