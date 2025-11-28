import json
from redis import Redis
from typing import Dict, List, Literal, Union

from stream import Stream, StreamType, StreamDataType

ConnectionType = Union[
    Literal["input"],
    Literal["output"],
    Literal["bidirectional"]
]


class StreamExistsError(Exception):
    pass

class StreamNotFoundError(Exception):
    pass


class Connection(object):
    __SERVICE_REGISTRY = "available_streams"

    __redis: Redis
    __registered_streams: Dict[str, Stream]
    __type: ConnectionType

    def __init__(self, host: str, port: int, type: ConnectionType) -> None:
        self.__redis = Redis(host, port)
        self.__type = type
        self.__registered_streams = {}

    def close(self):
        self.__redis.close()

    def publish(self, channel, data):
        self.__redis.publish(channel, data)

    def register_stream(self, name: str, type: StreamType, data_type: StreamDataType) -> Stream:
        stream_exists = self.__redis.hexists(self.__SERVICE_REGISTRY, name)

        if stream_exists:
            raise StreamExistsError(f"Stream with name {name} already exists")

        stream = Stream(self, name, type, data_type)
        self.__registered_streams[name] = stream

        self.__redis.hset(
            self.__SERVICE_REGISTRY,
            stream.name,
            json.dumps(stream.serialize())
        )

        return stream

    def unregister_stream(self, name) -> bool:
        self.__registered_streams.pop(name, None)
        num_deleted = self.__redis.hdel(self.__SERVICE_REGISTRY, name)

        return num_deleted > 0

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

    def get_stream(self, stream_name: str) -> Stream:
        stream_properties = self.__redis.hget(
            self.__SERVICE_REGISTRY,
            stream_name
        )

        if not stream_properties:
            raise StreamNotFoundError(f"Stream '{stream_name}' does not exist")

        stream_info = json.loads(stream_properties)

        return Stream(
            self,
            stream_name,
            stream_info["type"],
            stream_info["dataType"]
        )


    def stream_exists(self, stream_name: str) -> bool:
        return self.__redis.hexists(self.__SERVICE_REGISTRY, stream_name)

