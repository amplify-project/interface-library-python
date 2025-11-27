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

class Serializable(ABC):
    @abstractmethod
    def serialize(self):
        pass


class Stream(Serializable):
    def __init__(self, connection: Connection, name: str, type: StreamType, data_type: StreamDataType) -> None:
        self.__name = name
        self.__connection = connection

        self.__type = type
        self.__data_type = data_type

    def publish(self) -> None:
        pass

    def serialize(self):
        return {
            "name": self.__name,
            "type": self.__type,
            "dataType": self.__data_type
        }
