import pika
import sys
from log import logger


class adcp_io_rabbitmq:

    def __init__(self):
        self.connection = None
        self.channel = None
        self.exchange = ""
        self.queue_name = ""
        self.routing_key = "#"

    def connect(self, exchange, host="localhost", user="guest", pw="guest", routing_key="#"):
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
        self.routing_key = routing_key

        print("User: " + user)
        print("PW: " + pw)

        # Make the connection
        credentials = pika.PlainCredentials(user, pw)
        params = pika.ConnectionParameters(host=host, credentials=credentials)
        #params = pika.URLParameters('amqp://rico:test@192.168.0.124:5672/%2F')
        #self.connection = pika.BlockingConnection(parameters=params)

        # Attempt to connect to RabbitMQ based off params
        if self.rabbitmq_connect(params):

            if self.connection.is_open:
                logger.info("RabbitMQ connection opened.")

                self.connection.add_on_connection_blocked_callback(self.on_connected)

                # Set a channel with a random name
                self.channel = self.connection.channel()

                # Create a queue
                result = self.channel.queue_declare(exclusive=True)
                self.queue_name = result.method.queue

                # Create a Topic exchange if it does not exist
                self.channel.exchange_declare(exchange=self.exchange, type='topic')

        else:
            logger.error("RabbitMQ connection could not be made.")
            sys.exit()

    def rabbitmq_connect(self, params):
        try:
            self.connection = pika.BlockingConnection(parameters=params)
            if not self.connection.is_open:
                self.rabbitmq_connect(params)
        except pika.exceptions.ConnectionClosed as ex:
            logger.error("Error trying to connect to RabbitMQ", ex)
            return False
        except Exception as ex:
            logger.error("Error trying to connect to RabbitMQ", ex)
            return False

        return True


    def on_connected(self, frame):
        logger.error("RabbitMQ is low on resources: " + str(frame))

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

    def read(self):
        """
        Start the loop to wait for incoming messages.  Then handle the messages in handle_msg.
        """
        # Wait for messages
        self.channel.basic_consume(self.handle_msg, queue=self.queue_name)

    def handle_msg(self, channel, method, header, body):
        """
        Handle the incoming data from the routing key given.
        :param channel:
        :param method:
        :param header:
        :param body:
        :return:
        """
        logger.info("%s %s %s %s", channel, method, header, body)

    def close(self):
        self.connection.close()

