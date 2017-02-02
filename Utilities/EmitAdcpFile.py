import os.path
import sys
import getopt
import threading
import pickle
from log import logger
from Comm.EnsembleReceiver import EnsembleReceiver
from Codecs.AdcpCodec import AdcpCodec

from RabbitMQ.adcp_io_rabbitmq import adcp_io_rabbitmq


class EmitAdcpFile:
    """

    """

    def __init__(self, ens_in_burst, path):
        """
        Initialize the processor.  Get the number ensembles per burst and
        process the data and store the recorded MATLAB file to the path given.
        :param ens_in_burst: Number of ensembles per waves burst.
        :param path: File path to record the MATLAB file.
        """
        self.ens_receiver = None
        self.ens_reader = None

        # Codec to decode the data from the file
        self.codec = AdcpCodec(55057)
        self.codec.EnsembleEvent += self.process_ensemble_codec
        self.codec.enable_waveforce_codec(ens_in_burst, path, 32.123, 117.234, 1, 2, 3, 12.456)   # Enable WaveForce codec

        self.ens_count = 0
        self.ens_codec_count = 0

        self.prev_ens_num = 0
        self.missing_ens = 0

        self.rabbit = adcp_io_rabbitmq()
        self.rabbit.connect("ADCP")

    def process(self, file_path):
        """
        Read the file and start a thread to monitor the incoming ensembles.
        :param file_path: File  path the read files
        """
        # Create ensemble receiver
        self.ens_receiver = EnsembleReceiver()
        self.ens_receiver.EnsembleEvent += self.process_ensemble

        # Start thread to monitor incoming ensembles
        # Connect to ensemble server
        self.ens_reader = threading.Thread(name='EnsFileReader', target=self.ens_receiver.connect, args=[55057]).start()

        # Process the file
        self.process_file(file_path)

        # Stop the receiver
        self.ens_receiver.close()

        logger.info("Completed File reader")
        logger.info("Ensemble UDP Count: " + str(self.ens_count))
        if self.missing_ens > 0:
            logger.info("Missing Ensembles from UDP: " + str(self.missing_ens))
        logger.info("Ensemble Codec Count: " + str(self.ens_codec_count))

    def process_file(self, file_path):
        """
        Process the file given.  This read from the file
        and add it to the codec.  The codec will then decode
        the data and pass it to the UDP port.
        """
        # Check if the file exist
        if os.path.exists(file_path):

            logger.info("Open file: " + file_path)

            # Open the file
            f = open(file_path, "rb")

            # Add the data from the file to the codec
            data = f.read(4096)
            while len(data) > 0:
                # Add data to codec
                self.codec.add(data)

                # Read next block from the file
                data = f.read(4096)

            # Close the file
            f.close()
        else:
            logger.error("File does not exist")

    def process_ensemble(self, sender, ens):
        """
        Receive and process the incoming ensemble from the UDP port.
        This data has been processed through the codec then passed over
        the UDP port as JSON data.  The JSON datasets were then collected
        and assembled as a JSON ensemble.
        :param sender: Sender of the ensemble.
        :param ens: Ensemble data.
        """
        logger.debug("UDP: " + str(ens.EnsembleNumber))
        self.ens_count += 1

        # Check for missing ensembles
        if self.prev_ens_num > 0 and self.prev_ens_num + 1 != ens.EnsembleNumber:
            for msens in range((ens.EnsembleNumber - 1) - self.prev_ens_num):
                logger.info("Missing Ens: " + str(self.prev_ens_num + msens + 1) + " prev: " + str(self.prev_ens_num) + " cur: " + str(ens.EnsembleNumber)) # add 1 to msens because 0 based
                self.missing_ens += 1

        self.prev_ens_num = ens.EnsembleNumber

    def process_ensemble_codec(self, sender, ens):
        """
        Receive and process the incoming ensemble directly from the codec.
        This data was process and passed as an Ensemble object.
        :param sender: Sender of the ensemble.
        :param ens: Ensemble data.
        """
        if ens.IsEnsembleData:
            logger.debug("Codec: " + str(ens.EnsembleData.EnsembleNumber))
            self.ens_codec_count += 1

        # Publish to RabbitMQ
        self.emit_ens(ens)

    def emit_ens(self, ens):
        """
        Emit the ensemble data to the RabbitMQ.
        :param ens: Ensemble data.
        """
        serial = "0000"
        if ens.IsEnsembleData:
            serial = ens.EnsembleData.SerialNumber

        self.rabbit.send("adcp." + serial + ".data", pickle.dumps(ens))


def main(argv):
    """
    MAIN to run the application.
    """
    inputfile = ''
    verbose = False
    record_path = "recorder/"
    ens_in_burst = 1028
    try:
        opts, args = getopt.getopt(argv, "hvi:p:e:", ["ifile=", "path=", "ens=", "verbose"])
    except getopt.GetoptError:
        print('EmitAdcpFile.py -i <inputfile> -p <path> -e <ens_in_burst> -v')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('EmitAdcpFile.py -i <inputfile> -p <path> -e <ens_in_burst> -v')
            sys.exit()
        elif opt in ("-i", "--ifile"):
            inputfile = arg
        elif opt in ("-p", "--path"):
            record_path = arg
        elif opt in ("-e", "--ens"):
            ens_in_burst = arg
        elif opt in ("-v", "--verbose"):
            verbose = True
            print("Verbose ON")
    print('Input file is: ', inputfile)

    # Run report on file
    EmitAdcpFile(ens_in_burst, record_path).process(inputfile)

if __name__ == "__main__":
    main(sys.argv[1:])

