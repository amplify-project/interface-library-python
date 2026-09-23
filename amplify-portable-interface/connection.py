import json
import uuid
import threading
import time
from redis import Redis
from typing import Any, Dict, Literal, Union
from collections.abc import Callable

from .stream import Stream, StreamType, StreamDataType

ConnectionType = Union[
    Literal["input"],
    Literal["output"],
    Literal["io"]
]


class StreamExistsError(Exception):
    pass


class StreamNotFoundError(Exception):
    pass


class Connection(object):
    __SERVICE_REGISTRY = "available_streams"
    __DEVICE_REGISTRY = "connected_devices"

    __redis: Redis
    __registered_streams: Dict[str, Stream]
    __type: ConnectionType
    __device_info: Any
    __device_id: str
    __stop_heartbeat: threading.Event
    __heartbeat_thread: threading.Thread
    __pubsub_thread: Any

    def __init__(self, host: str, type: ConnectionType, device_info: Any = None) -> None:
        if ":" in host:
            h, p = host.split(":")
            self.__redis = Redis(host=h, port=int(p))
        else:
            self.__redis = Redis(host=host)

        self.__type = type
        self.__device_info = device_info
        self.__device_id = str(uuid.uuid4())

        self.__registered_streams = {}
        self.__pubsub = self.__redis.pubsub()
        self.__pubsub_thread = None

        self.__register_device()
        self.__stop_heartbeat = threading.Event()
        self.__heartbeat_thread = threading.Thread(target=self.__heartbeat_loop, daemon=True)
        self.__heartbeat_thread.start()

    @property
    def pubsub(self):
        return self.__pubsub

    def close(self):
        for stream in list(self.__registered_streams.values()):
            self.unregister_stream(stream)

        if self.__pubsub_thread:
            self.__pubsub_thread.stop()
            self.__pubsub_thread.join(timeout=1.0)

        self.__stop_heartbeat.set()
        self.__heartbeat_thread.join(timeout=1.0)
        self.__unregister_device()

        self.__redis.close()

    def register_stream(self, name: str, type: StreamType, data_type: StreamDataType) -> Stream:
        if self.stream_exists(name):
            raise StreamExistsError(f"Stream with name {name} already exists")

        stream = Stream(self, name, type, data_type)
        self.__registered_streams[name] = stream

        self.__redis.hset(
            self.__SERVICE_REGISTRY,
            stream.name,
            json.dumps(stream.serialize())
        )

        return stream

    def unregister_stream(self, stream: Stream) -> bool:
        streamObj = self.__registered_streams.pop(stream.name, None)

        if not streamObj:
            return False

        streamObj.unregister()
        num_deleted = self.__redis.hdel(self.__SERVICE_REGISTRY, streamObj.name)

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

    def __register_device(self):
        self.__redis.hset(
            self.__DEVICE_REGISTRY,
            self.__device_id,
            json.dumps({
                "type": self.__type,
                "info": self.__device_info,
                "lastSeen": time.time()
            })
        )

    def __unregister_device(self):
        self.__redis.hdel(self.__DEVICE_REGISTRY, self.__device_id)

    def __heartbeat_loop(self):
        while not self.__stop_heartbeat.wait(10):
            self.__register_device()

    def ensure_pubsub_thread(self):
        if self.__pubsub_thread is None or not self.__pubsub_thread.is_alive():
            self.__pubsub_thread = self.__pubsub.run_in_thread(sleep_time=0.01, daemon=True)

    def publish(self, channel: str, data: Any) -> None:
        self.__redis.publish(channel, json.dumps(data))

    def subscribe_by_name(self, stream_name: str, callback: Callable[[Any], None]) -> bool:
        stream = self.get_stream(stream_name)
        stream.subscribe(callback)

        return True
