# encoding: utf-8
from __future__ import print_function
from __future__ import unicode_literals

from hvlp.packets import *
from hvlp.defines import *

from abc import ABCMeta, abstractmethod
from six import with_metaclass

import threading
import socket
import logging


class Session(with_metaclass(ABCMeta)):
    """ Template class for all session's types """

    def __init__(self, *args, **kwargs):
        super(Session, self).__init__()
        self.args = args
        self.kwargs = kwargs

    @abstractmethod
    def run(self):
        raise NotImplementedError


class EchoSession(Session):
    """ Demo session that will return back any message sent by the client """

    def __init__(self, client, *args, **kwargs):
        self.client = client
        super(EchoSession, self).__init__(client, *args, **kwargs)

    def run(self):
        print('Connection from {0}'.format(self.client.getpeername()))
        while True:
            try:
                data = self.client.recv(1024)
                self.client.sendall(data)

            except socket.error:
                pass


class HvlpSession(Session):
    """ Hilscher Variable Protocol Length Session Class

    The session is a thread that works with a specific protocol version and handles the incoming
    packets from the client. The session is also responsible to forward incoming publish packets
    to all the subscribers of the topic in the `publish` packet.

    Args:
          client        : Client connection
          register      : Register instance that created the broker

    Attributes:
        self.client     : Client connection
        self.register   : Reference to the broker register
        self.state      : Session state
        self.terminate  : Stop signal for all threads in the session
        self.log        : Logger instance
        self.lock       : Lock instance for shared resources
        self.stream_in  : Input byte stream

    """

    WAIT_CONNECT = 0
    CONNECTED = 1
    ERROR_NO_DATA = 10035

    def __init__(self, client, register, *args, **kwargs):

        # Logging configuration
        self.log = logging.getLogger(self.__class__.__name__)
        self.log.addHandler(logging.NullHandler())

        # Connection attributes. A recv timeout lets the listener wake up
        # periodically to notice the terminate signal and detect EOF instead
        # of blocking forever on a dead socket.
        self.client = client
        self.client.settimeout(1)

        # Session management
        self.register = register
        self.state = self.WAIT_CONNECT
        self.terminate = threading.Event()

        # Shared resources
        self.lock = threading.Lock()
        self.stream_in = b""

        # Call the parent constructor
        super(HvlpSession, self).__init__(client, register, *args, **kwargs)

    ###############################################################################################

    def on_packet(self, packet):
        """ Packet handler that might be overridden by a custom session implementation

        Args:
            packet  : A packet object

        """

        self.log.debug("{0} from {1}".format(packet, self.client.getpeername()))

        # Handle subscribe packets
        if packet.id == SubscribePacket.ID:
            self.on_subscribe(packet.topics)

        # Handle unsubscribe packets
        elif packet.id == UnsubscribePacket.ID:
            self.on_unsubscribe(packet.topics)

        # Handle publish packets
        elif packet.id == PublishPacket.ID:
            self.on_publish(packet)

        # Handle unknown packets
        else:
            self.log.debug("UNEXPECTED PACKET: {0}".format(packet))

    ###############################################################################################

    def on_subscribe(self, topics):
        """ Subscribe the client on `subscribe` request

        Args:
            topics  : List of topic names

        """

        self.register.subscribe(topics=topics, client=self.client)
        self.log.debug(self.register)

    ###############################################################################################

    def on_unsubscribe(self, topics):
        """ Unsubscribe the client on `subscribe` request

        Args:
            topics  : List of topic names

        """

        self.register.unsubscribe(topics=topics, client=self.client)
        self.log.debug(self.register)

    ###############################################################################################

    def on_publish(self, packet):
        """ Forward publish packets to the topic subscribers

        Args:
            packet  : Packet object

        """

        # Get all subscribers and remove the publisher
        subscribers = self.register.get_subscribers(packet.topic)

        # Publish to all subscribers
        for subscriber in subscribers:

            # Skip the sender of the message
            if self.client == subscriber:
                continue

            # Send the message to the rest of the subscribers
            try:
                self.log.debug("PUBLISH to {0} with topic='{1}', data={2}".format(
                                subscriber.getpeername(),
                                packet.topic,
                                tuple(packet.data)
                            ))
                subscriber.sendall(packet.to_bytes())

            except socket.error:
                self.log.debug("Client not accessible, deleting from the register")
                self.register.unsubscribe(topics=[packet.topic], client=subscriber)

    ###############################################################################################

    def update(self, packet):
        """ Update the session state

        Args:
            packet  : Packet object

        """

        # STATE: WAIT_CONNECT
        if self.state == self.WAIT_CONNECT:

            # Request to connect to the broker
            if packet.id == ConnectPacket.ID:
                self.log.debug('CONNECT from {0}'.format(self.client.getpeername()))
                self.state = self.CONNECTED

            # Ignore packets until the `connect` packet is detected
            else:
                self.log.debug("UNEXPECTED PACKET {0} in state {1}".format(packet, self.state))

        # STATE: CONNECTED
        elif self.state == self.CONNECTED:

            # Request to disconnect from the broker
            if packet.id == DisconnectPacket.ID:
                self.log.debug('DISCONNECT from {0}'.format(self.client.getpeername()))
                self.terminate.set()

            # Perform the required operations on other packets
            else:
                self.on_packet(packet)

    ###############################################################################################

    def listen(self):
        """ Wait for new data and write it to the input stream """

        while not self.terminate.is_set():

            with self.lock:
                stream_length = len(self.stream_in)

            # Back-pressure: if the buffer is full, wait instead of spinning
            # on recv-less loop iterations.
            if stream_length >= MAX_STREAM_SIZE:
                self.terminate.wait(0.05)
                continue

            try:
                data = self.client.recv(BUFFER_SIZE)

                # An empty result means the peer closed the connection (EOF).
                # Without this the session would spin forever on a dead socket.
                if not data:
                    self.stop()
                    break

                with self.lock:
                    self.stream_in += data

            # A timeout just means no data arrived in the polling window.
            except socket.timeout:
                continue

            # Any other socket error: the connection is gone.
            except socket.error:
                self.stop()
                break

    ###############################################################################################

    def run(self):
        """ Session activity required by threading.Thread on start()

         1. Register the current session
         2. Start the message sniffer
         3. While `stop` signal is not active
             3.1. Copy the input buffer
             3.2. Parse a packet from the buffer
             3.3. Update the session state

        """

        self.log.debug("SESSION START")
        self.register.add_session(self)

        # Start the data capture thread
        listener = threading.Thread(target=self.listen)
        listener.daemon = True
        listener.start()

        while not self.terminate.is_set():

            # Extract the next complete frame, if one has fully arrived.
            with self.lock:
                frame_len = Packet.next_frame_length(self.stream_in)
                if frame_len is not None:
                    frame = bytes(self.stream_in[:frame_len])
                    self.stream_in = self.stream_in[frame_len:]
                else:
                    frame = None

            # No complete frame yet: wait briefly instead of busy-spinning.
            if frame is None:
                self.terminate.wait(0.02)
                continue

            # A malformed frame is already consumed above, so discard it and
            # resynchronise on the next frame instead of crashing or looping.
            try:
                packet = Packet.from_bytes(frame)
            except HvlpParsingError:
                self.log.debug("Discarding malformed frame")
                continue

            # Update the session state
            self.update(packet)

        self.cleanup()
        self.log.debug("SESSION END")

    ###############################################################################################

    def stop(self):
        """ Stop the current thread """
        self.terminate.set()

    ###############################################################################################

    def cleanup(self):
        """ Close the client connection and remove the session from the register """

        try:
            # Close the client connection
            self.client.shutdown(socket.SHUT_RDWR)
            self.client.close()

            # Remove session from the register
            self.register.remove_session(self)

            # Show the register before
            self.log.debug("Register before : {0}".format(self.register.get_sessions()))

            # Remove the client's subscriptions from the registry. get_topics
            # returns a snapshot set, so this does not iterate the register
            # while unsubscribe mutates it.
            topics = self.register.get_topics(self.client)
            if topics:
                self.register.unsubscribe(topics=topics, client=self.client)

            # Show the register after
            self.log.debug("Register after : {0}".format(self.register.get_sessions()))

        except socket.error:
            pass
