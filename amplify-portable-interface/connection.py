from redis import Redis
from stream import Stream
from typing import List


class Connection(object):
    __redis: Redis
    __streams: List[Stream]
    __type: str

    def __init__(self, host: str, port: int, type: str) -> None:
        self.__redis = Redis(host, port)
        self.__type = type
        self.__streams = []

    def register_stream(self, name: str, type: str, data_type: str) -> Stream:
        stream = Stream(self, name)
        self.__streams.append(stream)

        return stream
