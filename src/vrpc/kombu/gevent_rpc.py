import socket
from typing import MutableMapping, Any, Optional

import gevent
from gevent.event import AsyncResult
from gevent.timeout import Timeout
from kombu import Connection

from vrpc.typing import Result as TResult
from .rpc import BaseRPC


class Result(TResult):

    def __init__(self):
        self.async_result = AsyncResult()

    def result(self, timeout: Optional[float] = None) -> Any:
        return self.async_result.result(timeout)

    def exception(self, timeout: Optional[float] = None) -> Optional[Exception]:
        try:
            self.async_result.get(timeout=timeout)
        except Timeout:
            raise
        except Exception as ex:
            return ex
        else:
            return None

    def set_result(self, result: Any):
        self.async_result.set(result)

    def set_exception(self, exception: Exception, exc_info=None):
        self.async_result.set_exception(exception, exc_info)


class RPC:

    def __init__(self, connection: Connection):
        self.rpc = BaseRPC(connection)
        self.rpc.add_callback(self._callback)
        self._awaiting_results = {}

        self._should_stop = False
        self._gevent = gevent.spawn(self._consumer_run)

    def call(self, message: str, routing_key: str, properties: MutableMapping[str, str] = None) -> Result:
        correlation_id = self.rpc.call(message, routing_key, properties)
        result = Result()
        self._awaiting_results[correlation_id] = result
        return result

    def _callback(self, correlation_id, body):
        result = self._awaiting_results.pop(correlation_id)
        result.set_result(body)

    def _consumer_run(self):
        while not self._should_stop:
            try:
                self.rpc.drain_message(0.1)
            except socket.timeout:
                gevent.sleep(0)
