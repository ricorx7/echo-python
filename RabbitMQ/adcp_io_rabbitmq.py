import pika
from log import logger


class adcp_io_rabbitmq:

    def __init__(self):
        self.connection = None
        self.channel = None
        self.exchange = ""

    def connect(self, exchange, host='localhost', user='guest', pw='guest'):
        """
        Connect to the RabbitMQ server.  Use the exchange given
        to connect to a specific exchange.

        If connecting to RabbitMQ on the localhost, then the username:pw guest:guest will work.
        But if connecting remotely, a username and password must be given.
        :param exchange: Exchange to connect.
        :param host: Host address.
        :param user: Username.
        :param pw: Password.
        :return:
        """
        self.exchange = exchange

        # Make the connection
        credentials = pika.PlainCredentials(user, pw)
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host, credentials=credentials))

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

