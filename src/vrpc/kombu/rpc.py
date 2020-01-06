
import time
import uuid
from typing import MutableMapping, Set, Optional, Callable

from kombu import Connection, Producer, Consumer, Queue, Message


CorrelationID = str


class BaseRPC:

    def __init__(self, connection: Connection, consumer_connection: Optional[Connection] = None):
        self.connection = connection
        self.consumer_connection = consumer_connection or connection
        self.producer = Producer(self.connection)
        self.consumer = Consumer(
            self.consumer_connection,
            queues=[Queue(exclusive=True, auto_delete=True)],
            on_message=self._on_message
        )
        self.consumer.consume()
        self.queue: Queue = self.consumer.queues[0]
        self.callbacks = list()

        self._awaiting_results: Set[CorrelationID] = set()

    def call(self, message: str, routing_key: str, properties: MutableMapping[str, str] = None) -> CorrelationID:
        properties = properties or {}
        correlation_id = str(uuid.uuid4())
        properties['correlation_id'] = correlation_id
        properties['reply_to'] = self.queue.name

        self._awaiting_results.add(correlation_id)
        self.producer.publish(message, routing_key, **properties)

        return correlation_id

    def add_callback(self, cb: Callable[[CorrelationID, str], None]):
        self.callbacks.append(cb)

    def _on_message(self, message: Message):
        body = message.body
        correlation_id = message.properties['correlation_id']

        try:
            self._awaiting_results.remove(correlation_id)
        except KeyError:
            raise Exception("UNEXPECTED MESSAGE")

        for callback in self.callbacks:
            callback(correlation_id, body)

    def _drain_message(self, timeout=None):
        """ Drain a single event from the connection. """
        self.connection.drain_events(timeout=timeout)


class SimpleRPC(BaseRPC):

    def __init__(self, connection):
        BaseRPC.__init__(self, connection)
        self.results: MutableMapping[str, str] = dict()
        self.callbacks.append(self._callback)

    def get(self, correlation_id: str, timeout=None) -> str:
        """ Block and wait until we receive the expected result. """
        start = time.time()
        while correlation_id not in self.results:
            if timeout:
                delta = time.time() - start
                new_timeout = timeout - delta
            else:
                new_timeout = None
            self._drain_message(new_timeout)

        return self.results[correlation_id]  # we should pop?

    def _callback(self, correlation_id: str, body: str):
        self.results[correlation_id] = body
