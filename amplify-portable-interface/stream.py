from abc import ABC, abstractmethod
from typing import Literal, Union

from connection import Connection

StreamType = Literal["discrete", "continuous"]
StreamDataType = Literal["number", "string", "boolean"]

class Serializable(ABC):
    @abstractmethod
    def serialize(self):
        pass


class Stream(Serializable):
    def __init__(self, connection: Connection, name: str, type: StreamType, data_type: StreamDataType) -> None:
        self.__connection = connection

        self.__name = name
        self.__type = type
        self.__data_type = data_type

    @property
    def name(self) -> str:
        return self.__name

    @property
    def type(self):
        return self.__type

    @property
    def data_type(self):
        return self.__data_type

    def publish(self) -> None:
        pass

    def serialize(self):
        return {
            "name": self.__name,
            "type": self.__type,
            "dataType": self.__data_type
        }
