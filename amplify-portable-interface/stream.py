from typing import Literal, Union

from connection import Connection

StreamType = Union[
    Literal["discrete"],
    Literal["continuous"]
]

StreamDataType = Union[
    Literal["number"],
    Literal["string"],
    Literal["boolean"],
]

class Stream(object):
    def __init__(self, connection: Connection, name: str) -> None:
        self.__name = name
        self.__connection = connection

    def publish(self) -> None:
        pass
