
from kombu import Connection

from vrpc.kombu.gevent_rpc import RPC

from configuration import user, password, host


if __name__ == "__main__":
    queue_name = "test.simple_responder"
    connection = Connection(hostname=host, userid=user, password=password)
    rpc = RPC(connection)
    message = "Test"
    result = rpc.call(message, routing_key=queue_name)
    print(result.result())
