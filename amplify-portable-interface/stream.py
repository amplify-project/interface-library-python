from connection import Connection


class Stream(object):
    def __init__(self, connection: Connection, name: str) -> None:
        self.__name = name
        self.__connection = connection

    def publish(self) -> None:
        pass
