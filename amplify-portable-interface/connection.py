import json
from redis import Redis
from typing import List, Literal, Union

from stream import Stream, StreamType, StreamDataType

ConnectionType = Union[
    Literal["input"],
    Literal["output"],
    Literal["bidirectional"]
]

class Connection(object):
    __SERVICE_REGISTRY = "available_streams"

    __redis: Redis
    __streams: List[Stream]
    __type: ConnectionType

    def __init__(self, host: str, port: int, type: ConnectionType) -> None:
        self.__redis = Redis(host, port)
        self.__type = type
        self.__streams = []

    def close(self):
        self.__redis.close()

    def register_stream(self, name: str, type: StreamType, data_type: StreamDataType) -> Stream:
        stream = Stream(self, name, type, data_type)
        self.__streams.append(stream)

        self.__redis.set(
            self.__SERVICE_REGISTRY,
            json.dumps(stream.serialize())
        )

        return stream
