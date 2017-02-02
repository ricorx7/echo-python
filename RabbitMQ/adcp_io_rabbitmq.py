import pika
from log import logger


class adcp_io_rabbitmq:

    def __init__(self):
        self.connection = None
        self.channel = None
        self.exchange = ""

    def connect(self, exchange, host='localhost'):
        """
        Connect to the RabbitMQ server.  Use the exchange given
        to connect to a specific exchange.
        :param exchange: Exchange to connect.
        :param host: Host address.
        :return:
        """
        self.exchange = exchange

        # Make the connection
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))

        # Set a channel with a random name
        self.channel = self.connection.channel()

        # Create a Topic exchange if it does not exist
        self.channel.exchange_declare(exchange=exchange, type='topic')

    def send(self, routing_key, message):
        """
        Send the message with the given routing key.
        The routing key is how the message is filtered.
        Ex Routing Key:
        adcp.546.cmd
        adcp.546.data
        :param routing_key: Routing key to filter the message.
        :param message: Message to send.
        """
        self.channel.basic_publish(exchange=self.exchange, routing_key=routing_key, body=message)
        logger.debug(" [x] Sent %r:%r" % (routing_key, message))

    def close(self):
        self.connection.close()

