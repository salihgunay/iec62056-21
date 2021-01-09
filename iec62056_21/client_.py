import time
import logging

from iec62056_21 import messages, constants, transports, exceptions
from datetime import datetime, timedelta, date
from iec62056_21 import utils

# create logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)


# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# add formatter to ch
ch.setFormatter(formatter)

# add ch to logger
logger.addHandler(ch)


class Iec6205621Client:
    """
    A client class for IEC 62056-21. Only validated with meters using mode C.
    """

    BAUDRATES_MODE_C = {
        "0": 300,
        "1": 600,
        "2": 1200,
        "3": 2400,
        "4": 4800,
        "5": 9600,
        "6": 19200,
    }
    ALLOWED_MODES = [
        "readout",
        "programming",
        "binary",
        "manufacturer6",
        "manufacturer7",
        "manufacturer8",
        "manufacturer9",
    ]
    MODE_CONTROL_CHARACTER = {
        "readout": "0",
        "programming": "1",
        "binary": "2",
        "manufacturer6": "6",
        "manufacturer7": "7",
        "manufacturer8": "8",
        "manufacturer9": "9",
    }
    SHORT_REACTION_TIME = 0.02
    REACTION_TIME = 0.2

    def __init__(
        self,
        transport,
        device_address="",
        password="00000000",
        battery_powered=False,
        error_parser_class=exceptions.DummyErrorParser,
    ):

        self.transport = transport
        self.device_address = device_address
        self.password = password
        self.battery_powered = battery_powered
        self.identification = None
        self._switchover_baudrate_char = None
        self.manufacturer_id = None
        self.use_short_reaction_time = False
        self.error_parser = error_parser_class()
        self._current_baudrate: int = 300

        if self.transport.TRANSPORT_REQUIRES_ADDRESS and not self.device_address:
            raise exceptions.Iec6205621ClientError(
                f"The transported used ({self.transport}) requires a device address "
                f"and none was supplied."
            )

    @property
    def switchover_baudrate(self):
        """
        Shortcut to get the baud rate for the switchover.
        """
        return self.BAUDRATES_MODE_C.get(self._switchover_baudrate_char)

    def read_single_value(self, address, additional_data="1"):
        """
        Reads a value from an address in the device.

        :param address:
        :param additional_data:
        :return:
        """
        # TODO Can't find documentation on why the additional_data of 1 is needed.
        #  LIS-200 Specific?

        # TODO: When not using the additional data on an EMH meter we get an ack back.
        #   a bit later we get the break message. Is the device waiting?

        request = messages.CommandMessage.for_single_read(address, additional_data)
        logger.info(f"Sending read request: {request}")
        self.transport.send(request.to_bytes())

        response = self.read_response()

        if len(response.data) > 1:
            raise exceptions.TooManyValuesReturned(
                f"Read of one value returned {len(response.data)}"
            )
        if len(response.data) == 0:
            raise exceptions.NoDataReturned(f"Read returned no data")

        logger.info(f"Received response: {response}")
        # Just return the data, not in a list since it is just one.
        return response.data[0]

    def write_single_value(self, address, data):
        """
        Writes a value to an address in the device.

        :param address:
        :param data:
        :return:
        """

        request = messages.CommandMessage.for_single_write(address, data)
        logger.info(f"Sending write request: {request}")
        self.transport.send(request.to_bytes())

        ack = self._recv_ack()
        if ack == constants.ACK:
            logger.info(f"Write request accepted")
            return
        elif ack == constants.NACK:
            # TODO: implement retry and raise proper error.
            raise ValueError(f"Received NACK upon sending {request}")
        else:
            raise ValueError(
                f"Received invalid response {ack} to write request {request}"
            )

    def connect(self):
        """
        Connect to the device
        """
        self.transport.connect()

    def disconnect(self):
        """
        Close connection to device
        """
        self.transport.disconnect()

    def startup(self):
        """
        Initial communication to start the session with the device. Sends a
        RequestMessage and receives identification message.
        """

        if self.battery_powered:
            self.send_battery_power_startup_sequence()
        logger.info("Staring init sequence")
        self.send_init_request()

        ident_msg = self.read_identification()

        # Setting the baudrate to the one propsed by the device.
        self._switchover_baudrate_char = ident_msg.switchover_baudrate_char
        self.identification = ident_msg.identification
        self.manufacturer_id = ident_msg.manufacturer

        # If a meter transmits the third letter (last) in lower case, the minimum
        # reaction time for the device is 20 ms instead of 200 ms.
        if self.manufacturer_id[-1].islower():
            self.use_short_reaction_time = True

    def access_programming_mode(self):
        """
        Goes through the steps to set the meter in programming mode.
        Returns the password challenge request to be acted on.
        """

        self.startup()

        self.ack_with_option_select("programming")

        # receive password request
        pw_req = self.read_response()

        return pw_req

    def standard_readout(self):
        """
        Goes through the steps to read the standard readout response from the device.
        """
        self.startup()
        self.ack_with_option_select("readout")
        logger.info(f"Reading standard readout from device.")
        response = self.read_response()
        return response

    def read_profile(self, start_date: date, end_date: date):
        """
        Reads some profile
        """
        if not (isinstance(start_date, date) or isinstance(end_date, date)):
            assert "Not proper date for reading profile"

        self.startup()
        self.ack_with_option_select("programming")

        response = self.read_response()
        if not utils.bcc_valid(response.to_bytes()):
            assert "Not a valid bcc"

        self._send_profile_request(start_date, end_date)
        response = self.read_response()  # Result
        if not utils.bcc_valid(response.to_bytes()):
            assert "Not a valid bcc"
        return self._arrange_profile_data(response)

    def _arrange_profile_data(self, response):
        if self.manufacturer_id == "MSY":
            return self._arrange_profile_data_makel(response)
        elif self.manufacturer_id == "LUN":
            return self._arrange_profile_data_luna(response)

    def _arrange_profile_data_luna(self, response):
        data: messages.DataSet
        idx = 0
        values = []
        temp_data = []
        for data in response.data[1:]:
            if idx % 14 == 0 and idx != 0:
                date_ = self.convert_makel_date(f'{temp_data[0].replace("-", "")[2:]}{temp_data[1].replace(":", "")}')
                values.append(messages.ProfileData(date=date_, f180=temp_data[2].replace("*kWh", "")))
                temp_data.clear()
            temp_data.append(data.value)
            idx += 1
        return values

    def _arrange_profile_data_makel(self, response):
        data: messages.DataSet
        idx = 0
        values = []
        temp_data = []
        for data in response.data[1:]:
            if idx % 9 == 0 and idx != 0:
                date_ = self.convert_makel_date(temp_data[0])
                values.append(messages.ProfileData(date_, *temp_data[1:]))
                temp_data = []
            temp_data.append(data.value)
            idx += 1
        return values

    @staticmethod
    def convert_makel_date(date_: str) -> datetime:
        """
        Convert date string to datetime object
        2101050800 -> 2021-01-05 08:00:00
        """
        if not isinstance(date_, str):
            assert "None proper type for date"
        if len(date_) != 10:
            assert "None proper length for string date"
        return datetime(year=int(f'20{date_[:2]}'), month=int(date_[2:4]), day=int(date_[4:6]), hour=int(date_[6:8]),
                        minute=int(date_[8:10]))

    def _send_profile_request(self, start_date: date, end_date: date):
        """
        Send profile request between dates
        """
        # Set format for Luna
        if self.manufacturer_id == "LUN":
            start_date_ = f"{start_date.year % 100}{start_date.month:02d}{start_date.day:02d}"
            end_date_ = f"{end_date.year % 100}{end_date.month:02d}{end_date.day:02d}"
        elif self.manufacturer_id == "MSY":  # Set format for makel
            start_date_ = f"{start_date.year % 100}{start_date.month:02d}{start_date.day:02d}0000"
            end_date_ = f"{end_date.year % 100}{end_date.month:02d}{end_date.day:02d}0000"

        command = "R"
        command_type = "5" if self.manufacturer_id == "LUN" else "2"
        address = "P1" if self.manufacturer_id == "LUN" else "P.01"
        end = "" if self.manufacturer_id == "LUN" else None

        cmd = messages.CommandMessage(command=command, command_type=command_type,
                                      data_set=messages.DataSet(value=f"{start_date_};{end_date_}", address=address, end=end))
        logger.info(f"Sending profile request to meter. {cmd.to_bytes()}")
        self.transport.send(cmd.to_bytes())

    def send_password(self, password=None):
        """
        On receiving the password challenge request one must handle the password
        challenge according to device specification and then send the password.
        :param password:
        """
        _pw = password or self.password
        data_set = messages.DataSet(value=_pw)
        cmd = messages.CommandMessage(command="P", command_type="1", data_set=data_set)
        logger.info("Sending password to meter")
        self.transport.send(cmd.to_bytes())

    def send_break(self):
        """
        Sending the break message to indicate that one wants to stop the
        communication.
        """
        logger.info("Sending BREAK message to end communication")
        break_msg = messages.CommandMessage(
            command="B", command_type="0", data_set=None
        )
        self.transport.send(break_msg.to_bytes())

    def ack_with_option_select(self, mode):
        """
        After receiving the identification one needs to respond with an ACK including
        the different options for the session. The main usage is to control the
        mode. readout, programming, or manufacturer specific. The baudrate change used
        will be the one proposed by the device in the identification message.

        :param mode:
        """
        # TODO: allow the client to suggest a new baudrate to the devices instead of
        #  the devices proposed one.

        mode_char = self.MODE_CONTROL_CHARACTER[mode]

        ack_message = messages.AckOptionSelectMessage(
            mode_char=mode_char, baud_char=self._switchover_baudrate_char
        )
        logger.info(f"Sending AckOptionsSelect message: {ack_message.to_bytes()}")
        self.transport.send(ack_message.to_bytes())
        self.rest()
        self.transport.switch_baudrate(
            baud=self.BAUDRATES_MODE_C[self._switchover_baudrate_char]
        )

    def send_init_request(self):
        """
        The init request tells the device they you want to start a session with it.
        When using the optical interface on the device there is no need to send the
        device address in the init request since there can be only one meter.
        Over TCP or bus-like transports like RS-485 you will need to specify the meter
        you want to talk to by adding the address in the request.

        Sending -> b'/?!\r\n'
        """
        request = messages.RequestMessage(device_address=self.device_address)
        logger.info(f"Sending request message: {request}")
        self.transport.send(request.to_bytes())
        self.rest()

    def read_identification(self):
        """
        Properly receive the identification message and parse it.
        """

        data = self.transport.simple_read(start_char="/", end_char="\x0a")

        identification = messages.IdentificationMessage.from_bytes(data)
        logger.info(f"Received identification message: {identification}")
        return identification

    def send_battery_power_startup_sequence(self, fast=False):
        """
        Battery powered devices require a startup sequence of null bytes to
        activate
        There is a normal and a fast start up sequence defined in the protocol.

        Normal:
            Null chars should be sent to the device for 2.1-2.3 seconds with a maximum
            of 0,5 seconds between them.
            After the last charachter the client shall wait 1.5-1,7 seconds until it
            sends the request message

        :param fast:
        """
        if fast:
            raise NotImplemented("Fast startup sequence is not yet implemented")

        timeout = 2.2
        duration = 0
        start_time = time.time()
        logger.info("Sending battery startup sequence")
        while duration < timeout:
            out = b"\x00"
            self.transport.send(out)
            self.rest(0.2)
            duration = time.time() - start_time
        logger.info("Startup Sequence finished")

        self.rest(1.5)

    def _recv_ack(self):
        """
        Simple way of receiving an ack or nack.
        """
        ack = self.transport.recv(1).decode(constants.ENCODING)
        return ack

    def read_response(self):
        """
        Reads the response from a device and parses it to the correct message type.

        """
        data = self.transport.read()
        if data.startswith(b"\x01"):
            # We probably received a password challenge
            return messages.CommandMessage.from_bytes(data)
        else:
            response = messages.AnswerDataMessage.from_bytes(data)
            self.error_parser.check_for_errors(response)
            return response

    @property
    def reaction_time(self):
        """
        The device can define two different reaction times. Depending if the third
        letter in the manufacturer ID in the identification request is in lower case the
        shorter reaction time is used.
        """
        if self.use_short_reaction_time:
            return self.SHORT_REACTION_TIME
        else:
            return self.REACTION_TIME

    def rest(self, duration=None):
        """
        The protocol needs some timeouts between reads and writes to enable the device
        to properly parse a message and return the result.
        """

        _duration = duration or (self.reaction_time * 1.25)
        logger.debug(f"Resting for {_duration} seconds")
        time.sleep(_duration)

    @classmethod
    def with_serial_transport(
        cls,
        port,
        device_address="",
        password="00000000",
        battery_powered=False,
        error_parser_class=exceptions.DummyErrorParser,
    ):
        """
        Initiates the client with a serial transport.

        :param port:
        :param device_address:
        :param password:
        :param battery_powered:
        :return:
        """
        transport = transports.SerialTransport(port=port)
        return cls(
            transport, device_address, password, battery_powered, error_parser_class
        )

    @classmethod
    def with_tcp_transport(
        cls,
        address,
        device_address="",
        password="00000000",
        battery_powered=False,
        error_parser_class=exceptions.DummyErrorParser,
    ):
        """
        Initiates the client with a TCP Transport.

        :param address:
        :param device_address:
        :param password:
        :param battery_powered:
        :return:
        """
        transport = transports.TcpTransport(address=address)
        return cls(
            transport, device_address, password, battery_powered, error_parser_class
        )
