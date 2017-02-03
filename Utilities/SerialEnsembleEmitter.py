import sys
import getopt
import threading
import socket
import jsonpickle
from Comm.AdcpSerialPortServer import AdcpSerialPortServer
from log import logger
from Codecs.AdcpCodec import AdcpCodec

from RabbitMQ.rabbitmq_topic import rabbitmq_topic


class SerialEnsembleEmitter:

    def __init__(self):
        self.serial_server = None
        self.serial_server_thread = None
        self.rabbit = None
        self.raw_serial_socket = None
        self.is_alive = True
        self.codec = None

    def connect(self, rabbit_url, rabbit_user, rabbit_pw, comm_port, baud, tcp_port=55056):

        # Create a RabbitMQ connection
        self.rabbit = rabbitmq_topic.connect("ADCP", rabbit_url, rabbit_user, rabbit_pw)

        # Create an ADCP codec
        self.codec = AdcpCodec()
        self.codec.EnsembleEvent += self.process_ensemble

        # Create an ADCP Serial port connection
        self.serial_server = AdcpSerialPortServer(tcp_port,
                                                  comm_port,
                                                  baud)

        # Start a tcp connection to monitor incoming data and record
        self.serial_server_thread = threading.Thread(name='AdcpWriter',
                                                     target=self.create_raw_serial_socket(self.tcp_port))
        self.serial_server_thread.start()

    def create_raw_serial_socket(self, port):
        """
        Connect to the ADCP serial server.  This TCP server outputs data from
        the serial port.  Start reading the data.
        """
        try:
            # Create socket
            self.raw_serial_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.raw_serial_socket.connect(('localhost', int(port)))
            self.raw_serial_socket.settimeout(1)    # Set timeout to stop thread if terminated
        except ConnectionRefusedError as err:
            logger.error("Serial Send Socket: ", err)
        except Exception as err:
            logger.error('Serial Send Socket: ", Error Opening socket', err)

        # Start to read the raw data
        self.read_tcp_socket()

    def read_tcp_socket(self):
        """
        Read the data from the TCP port.  This is the raw data from the serial port.
        Send it to the codec to be decoded.
        """
        while self.is_alive:
            try:
                # Read data from socket
                data = self.raw_serial_socket.recv(4096)

                # If data exist process
                if len(data) > 0:
                    self.codec.add(data)

            except socket.timeout:
                # Just a socket timeout, continue on
                pass
            except Exception as e:
                logger.error("Exception in reading data.", e)
                self.stop_adcp_server()

        print("Read Thread turned off")

    def process_ensemble(self, sender, ens):
        """
        Receive an ensemble from the codec.  Then pass it to the emitter to
        pass it to the RabbitMQ server.
        :param sender: Sender of the ensemble.
        :param ens: Ensemble data.
        :return:
        """
        self.emit_ens(ens)

    def emit_ens(self, ens):
        """
        Emit the ensemble data to the RabbitMQ.
        The ensemble data will be pickled using JSON Pickle.
        :param ens: Ensemble data.
        """
        serial = "0000"
        if ens.IsEnsembleData:
            serial = ens.EnsembleData.SerialNumber

        self.rabbit.send("adcp." + serial + ".data.live", jsonpickle.dumps(ens))


def main(argv):
    """
    MAIN to run the application.
    """
    url = "localhost"
    user = "guest"
    password = "guest"
    comm_port = ""
    baud=115200
    try:
        opts, args = getopt.getopt(argv, "hu:c:p:t:b:", ["url=", "user=", "pw=", "verbose"])
    except getopt.GetoptError:
        print('SerialEnsembleEmitter.py -u <url> -c <username> -p <password> -t <comm_port> -b <baud>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('SerialEnsembleEmitter.py -u <url> -c <username> -p <password> -t <comm_port> -b <baud>')
            sys.exit()
        elif opt in ("-b", "--baud"):
            baud = arg
        elif opt in ("-t", "--port"):
            comm_port = arg
        elif opt in ("-u", "--url"):
            url = arg
        elif opt in ("-c", "--user"):
            user = arg
        elif opt in ("-w", "--password"):
            password = arg
    print('Comm Port: ', comm_port)
    print('Baud Rate: ', baud)
    print('RabbitMQ URL: ', url)
    print('RabbitMQ User: ', user)
    print("Available Serial Ports:")
    serial_list = AdcpSerialPortServer.list_serial_ports()

    # Verify a good serial port was given
    if comm_port in serial_list:
        SerialEnsembleEmitter.connect(rabbit_url=url, rabbit_user=user, rabbit_pw=password, comm_port=comm_port, baud=baud)

if __name__ == "__main__":
    main(sys.argv[1:])