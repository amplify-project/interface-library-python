from abc import ABC, abstractmethod
from typing import Literal, Union, Any
from collections.abc import Callable

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
        self.__callback = lambda _: None
        self.__is_subscribed = False

    @property
    def name(self) -> str:
        return self.__name

    @property
    def type(self):
        return self.__type

    @property
    def data_type(self):
        return self.__data_type

    def publish(self, data: Any) -> None:
        self.__connection.publish(
            self.__name,
            data
        )

    def subscribe(self, callback: Callable[[Any], None]):
        self.__callback = callback
        self.__connection.pubsub.subscribe(
            self.__name,
            self.__callback
        )

        self.__is_subscribed = True

    def unsubscribe(self) -> bool:
        if not self.__is_subscribed:
            return False

        self.__connection.pubsub.unsubscribe(self.__name)
        self.__is_subscribed = False

        return True

    def serialize(self):
        return {
            "name": self.__name,
            "type": self.__type,
            "dataType": self.__data_type
        }
