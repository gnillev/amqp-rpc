from socket import timeout
from kombu import Connection, Message, Queue

from configuration import host, user, password


class SimpleResponderService:
    """ Simple service that responds with requested message with a standard "Reply: " prefix. """

    def __init__(self, connection: Connection):
        self.connection = connection
        self.producer = self.connection.Producer()
        self.consumer = self.connection.Consumer(queues=Queue(queue_name), on_message=self.respond)
        self.consumer.consume()
        self._should_stop = False

    def start(self):
        while not self._should_stop:
            try:
                self.connection.drain_events(timeout=1)
            except timeout:
                continue

    def stop(self):
        self._should_stop = True

    def respond(self, message: Message):
        body = message.body
        properties = message.properties
        message.ack()  # For test/example, ack early
        print(f"Received message: {body}")

        reply_to = properties.get('reply_to')
        if reply_to:
            response = f"Reply: {body}"
            print(f"Replying to {reply_to}")
            self.producer.publish(response, reply_to, **properties)


if __name__ == "__main__":
    queue_name = "test.simple_responder"
    connection = Connection(hostname=host, userid=user, password=password)
    responder = SimpleResponderService(connection)
    responder.start()
