import json
from redis import Redis
from typing import Dict, List, Literal, Union

from stream import Stream, StreamType, StreamDataType

ConnectionType = Union[
    Literal["input"],
    Literal["output"],
    Literal["bidirectional"]
]

class Connection(object):
    __SERVICE_REGISTRY = "available_streams"

    __redis: Redis
    __registered_streams: List[Stream]
    __type: ConnectionType

    def __init__(self, host: str, port: int, type: ConnectionType) -> None:
        self.__redis = Redis(host, port)
        self.__type = type
        self.__registered_streams = []

    def close(self):
        self.__redis.close()

    def register_stream(self, name: str, type: StreamType, data_type: StreamDataType) -> Stream:
        stream = Stream(self, name, type, data_type)
        self.__registered_streams.append(stream)

        self.__redis.hset(
            self.__SERVICE_REGISTRY,
            stream.name,
            json.dumps(stream.serialize())
        )

        return stream

    def get_available_streams(self) -> Dict[str, Stream]:
        stream_properties = self.__redis.hgetall(self.__SERVICE_REGISTRY)
        streams = {}

        for name, value in stream_properties.items():
            stream_info = json.loads(value)

            streams[name] = Stream(
                self,
                name,
                stream_info["type"],
                stream_info["dataType"]
            )

        return streams
