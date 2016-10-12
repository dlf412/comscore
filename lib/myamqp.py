import time
import socket
from amqp.exceptions import ConnectionForced
from kombu import Connection, Exchange, Queue



class Amqp(object):

    def __init__(self, url, exchange, queue, routing_key):

        self.conn = Connection(url)
        self.exchange = Exchange(exchange, 'direct')
        self.routing_key = routing_key
        self.queue = Queue(queue, self.exchange, self.routing_key)

        self.producer = None
        self.consumer = None

    def send(self, obj, retry=False, retry_policy={}):
        if not self.producer:
            self.producer = self.conn.Producer()
        self.producer.publish(obj, exchange=self.exchange,
                              routing_key=self.routing_key,
                              declare=[self.queue],
                              retry=retry,
                              retry_policy=retry_policy,
                              serializer='json', compression='zlib')

    def poll(self, cb_func, errback=None, max_retries=None):
        if not self.consumer:
            self.consumer = self.conn.Consumer(self.queue,
                                               callbacks=[cb_func])
            self.consumer.qos(prefetch_count=1)
        self.consumer.consume()
        while True:
            try:
                self.conn.drain_events()
            except (socket.error, ConnectionForced):
                self.conn.ensure_connection(errback=errback,
                                            max_retries=max_retries)
                self.consumer = self.conn.Consumer(self.queue,
                                                   callbacks=[cb_func])
                self.consumer.consume()

    def _release(self):
        if self.consumer:
            self.consumer.close()
            self.consumer = None
        if self.producer:
            self.producer.close()
            self.producer = None
        if self.conn:
            self.conn.release()
            self.conn = None

    def __enter__(self):
        return self

    def __exit__(self, exec_type, exc_value, traceback):
        self._release()


def process_task(body, message):
    print type(body), body
    time.sleep(1)
    message.ack()

if __name__ == '__main__':
    with Amqp("amqp://127.0.0.1:5672", "123", "video", "video2") as q:
        q.send({"fuck":"123"})
        q.poll(process_task)

