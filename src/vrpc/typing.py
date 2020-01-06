from typing import Any, Optional, MutableMapping
from typing_extensions import Protocol


class Result(Protocol):

    def result(self, timeout: Optional[float]) -> Any:
        raise NotImplementedError()

    def exception(self, timeout: Optional[float]) -> Optional[Exception]:
        raise NotImplementedError()

    def set_result(self, result: Any):
        raise NotImplementedError()

    def set_exception(self, exception: Exception):
        raise NotImplementedError()


class RPC(Protocol):

    def call(self, message: str, routing_key: str, properties: MutableMapping[str, str] = None) -> Result: ...
