from abc import ABC, abstractmethod
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

class Stream(Serializable):
    def __init__(self, connection: Connection, name: str, type: StreamType, data_type: StreamDataType) -> None:
        self.__name = name
        self.__connection = connection

        self.__type = type
        self.__data_type = data_type

    def publish(self) -> None:
        pass
