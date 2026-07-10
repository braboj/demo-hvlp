# coding: utf-8
from __future__ import print_function
from __future__ import unicode_literals

from hvlp.packets import *
from hvlp.defines import *
from hvlp.logger import *
from hvlp.errors import *

from six.moves import input
import socket
import threading
import logging


class HvlpClient(threading.Thread):
    """Hilscher Variable Length Protocol Client API

    The client sends requests to the broker and listens for `publish` packets.

    Args:
        srv_addr        : Broker IP address
        port            : Broker port

    Attributes:
        srv_addr        : Broker address
        port            : Broker port
        sock            : Client socket

    """

    def __init__(self, srv_addr='localhost', port=DEFAULT_PORT):
        super(HvlpClient, self).__init__()

        # Logging configuration
        self.log = logging.getLogger(self.__class__.__name__)
        self.log.addHandler(logging.NullHandler())

        # Connection related attributes
        self.srv_addr = srv_addr
        self.port = port
        self.sock = None

        # Message queue
        self.lock = threading.Lock()
        self.stream_in = b""

    ###############################################################################################

    def open(self):
        """ Open a TCP socket connection

        Raises:
            HvlpConnectionError : When the broker doesn't respond to SYN packets

        """

        try:

            # Connect to the broker only once
            if self.sock:
                raise HvlpAlreadyConnected

            # Open the TCP connection
            self.sock = socket.socket()
            self.sock.connect((self.srv_addr, self.port))

            # Configure the socket to be non-blocking
            self.sock.settimeout(1)

        # The broker is not up
        except socket.error:
            raise HvlpConnectionError

    ###############################################################################################

    def send(self, *data):
        """ Send raw data over the TCP connection

        Args:
            data    : Bytes as arguments

        Raises:
            HvlpConnectionError : When the client cannot send data to the broker

        Example:

            # Send a raw connect packet
            client.send(1, 0)

            # Send a raw `subscribe` packet with topic 'a'
            client.send(3, 2, 1, 0x61)

        """

        stream = []
        try:

            # Convert to bytes
            for element in data:
                stream.append(int(element))

            # Open the TCP connection
            self.sock.sendall(bytearray(stream))

        # Socket attribute missing or connection error
        except (AttributeError, socket.error):
            raise HvlpConnectionError

    ###############################################################################################

    def close(self):
        """ Close the TCP connection

        Raises:
            HvlpConnectionError : When the broker doesn't respond to FIN packets
        """

        try:
            self.sock.shutdown(socket.SHUT_RDWR)
            self.sock.close()
            self.sock = None

        # Socket attribute missing or connection error
        except (AttributeError, socket.error):
            raise HvlpConnectionError

    ###############################################################################################

    def connect(self, payload=""):
        """ Send a `connect` request packet to the broker

        Args:
            payload    : Bytes as arguments

        Raises:
            HvlpConnectionError : When the socket is not initialized or the broker is not alive

        Example:

            # Send a default connect request with no payload
            client.connect()

            # Send a `connect` packet with optional data in the payload section
            clint.connect('Branko')

        """

        try:

            # Send the `connect` packet to the broker
            payload = payload.encode('utf-8')
            request = ConnectPacket(payload=payload).to_bytes()
            self.sock.sendall(request)

        except (AttributeError, socket.error):
            raise HvlpConnectionError

    ###############################################################################################

    def disconnect(self, payload=""):
        """ Send a `connect` request packet to the broker

        Args:
            payload    : Bytes as arguments

        Raises:
            HvlpConnectionError : When the socket is not initialized or the broker is not alive

        Example:

            # Send a default connect request with no payload
            client.disconnect()

            # Send a `connect` packet with optional data in the payload section
            clint.disconnect('Branko')

         """

        try:

            # Send the packet
            payload = payload.encode('utf-8')
            request = DisconnectPacket(payload).to_bytes()
            self.sock.sendall(request)

        # Socket related errors
        except (AttributeError, socket.error):
            raise HvlpConnectionError

    ###############################################################################################

    def subscribe(self, *topics):
        """ Send a `subscribe` request to the broker

        Args:
            topics    : Comma-separated topic names

        Raises:
            HvlpConnectionError : When the socket is not initialized or the broker is not alive

        Example:

            # Send a `subscribe` packet for topics 'a', 'b' and 'c'
            client.subscribe('a', 'b', 'c')

        """

        if not topics:
            raise HvlpArgumentsError

        try:
            request = SubscribePacket(topics=topics).to_bytes()
            self.sock.sendall(request)

        # Socket related errors
        except (AttributeError, socket.error):
            raise HvlpConnectionError

    ###############################################################################################

    def unsubscribe(self, *topics):
        """ Send an `unsubscribe` request to the broker

        Args:
            topics    : Comma-separated topic names

        Raises:
            HvlpConnectionError : When the socket is not initialized or the broker is not alive

        Example:

            # Send an `unsubscribe` packet for topics 'a', 'b', 'c'
            client.unsubscribe('a', 'b', 'c')

        """

        if not topics:
            raise HvlpArgumentsError

        try:
            request = UnsubscribePacket(topics=topics).to_bytes()
            self.sock.sendall(request)

        # Socket related errors
        except (AttributeError, socket.error):
            raise HvlpConnectionError

    ###############################################################################################

    def publish(self, topic, *data):
        """ Send a `publish` request to the broker

        Args:
            topic    : The topic name
            data     : Comma-separated list of bytes (range 0..255)

        Raises:
            HvlpConnectionError : When the socket is not initialized or the broker is not alive

        Example:

            # Send a `publish` packet with topic 'a' and the bytes 1 and 2
            client.publish('a', 1, 2)

        """

        if not (topic and data):
            raise HvlpArgumentsError

        try:

            # Convert to bytes
            data = [int(x) for x in data]

            request = PublishPacket(topic=topic, data=data).to_bytes()
            self.sock.sendall(request)

        # Bad payload format
        except ValueError:
            raise HvlpPayloadFormatError

        # Socket related errors
        except (AttributeError, socket.error):
            raise HvlpConnectionError

    ###############################################################################################

    def listen(self, stop):
        """ Save raw messages from the broker into the input buffer

        Args:
            stop     : Threading event to stop the listener loop

        Example:

            # Create the client object
            client = HvlpClient()

            # Create the stop event
            stop = threading.Event()

            # Start the listener thread
            listener = threading.Thread(target=client.listen, args=[stop, ])

        """

        while not stop.is_set():

            try:
                data = self.sock.recv(BUFFER_SIZE)

                # An empty result means the broker closed the connection.
                if not data:
                    break

                # Accumulate: a packet may span multiple recv calls, so append
                # instead of overwriting any buffered partial frame.
                self.stream_in += data

                # Drain every complete frame currently buffered.
                while True:
                    frame_len = Packet.next_frame_length(self.stream_in)

                    # Wait for the rest of a partial frame
                    if frame_len is None:
                        break

                    frame = bytes(self.stream_in[:frame_len])
                    self.stream_in = self.stream_in[frame_len:]

                    # A malformed frame is already consumed; log and resync.
                    try:
                        packet = Packet.from_bytes(frame)
                    except HvlpError as e:
                        self.log.error(e)
                        continue

                    # Call the packet handler
                    self.on_packet(packet)

            # Socket timed out (non-blocking / timeout mode): keep waiting.
            except socket.timeout:
                pass

            # Other socket related errors
            except (socket.error, AttributeError):
                break

    ###############################################################################################

    def on_packet(self, packet):
        """ Default packet handler that can be overridden by a custom application

        Args:
            packet     : Packet object

        Example:

            class MyHvlpClient(HvlpClient):

                def on_packet(self, packet):

                    if packet.id == PublishPacket.ID:
                        self.on_publish(packet)

                    elif packet.id == SomeCustomPacket.ID:
                        self.on_custom_packet(packet)

                    ...

        """

        self.log.info("{0} from {1}:{2}".format(packet, self.srv_addr, self.port))


###################################################################################################
# APPLICATION
###################################################################################################

class HvlpClientApp(HvlpClient):
    """ Hilscher Variable Protocol Length Client Application
    
    The client application extends the Client API to allow an application to be run by the user
    in an interactive mode. 
    
    Args:
        srv_addr    : The broker address to connect to
        port        : The port on which the broker is running
        
    Attributes:
        packets_in  : List used to save incoming packets
        terminate   : Signal used to stop all the threads in the application
        listener    : Thread instance used to listen for packets and save them in `packets_in`

    Examples:

        # Start the client application
        client = HvlpClientApp()
        client.run()

    """

    # Prompt symbol used to indicate that the application is waiting for a new command
    PROMPT = "> "

    def __init__(self, srv_addr='localhost', port=65432):
        super(HvlpClientApp, self).__init__(srv_addr=srv_addr, port=port)

        self.packets_in = []
        self.terminate = threading.Event()
        self.listener = None

    ###############################################################################################

    def config(self, srv_addr, port):
        """ Configure the server address and port

        Args:
            srv_addr  : IP address of the broker
            port      : Port of the target broker

        Raises:
            HvlpArgumentsError  : When either the broker address or the port is missing

        """

        try:
            self.srv_addr = srv_addr
            self.port = int(port)

        except Exception:
            raise HvlpArgumentsError

    ###############################################################################################

    def stop(self):
        """ Stop all the threads """
        self.terminate.set()

    ###############################################################################################

    def quit(self):
        """ Terminate all threads and quit the program """

        if self.listener:
            self.terminate.set()
            self.listener.join()

    ###############################################################################################

    def wait(self):
        """ Wait for user input

        Raises:
            HvlpCommandError    : When the user hits ENTER without specifying a command

        """

        # Split the command line into tokens
        try:
            self.show(self.PROMPT, end='')
            line = input()
            tokens = line.split()
            command = tokens[0]
            args = tokens[1:]

        except BaseException:
            raise HvlpCommandError

        return command, args

    ###############################################################################################

    def show(self, *args, **kwargs):
        """ Thread safe print """

        with self.lock:
            print(*args, **kwargs)

    ###############################################################################################

    def help(self, command=""):
        """ Display a help message """

        help_string = """

        -------------------------------------------------------------------------------------------
        | Command         | Description                               | Example                   |
        -------------------------------------------------------------------------------------------
        | connect         | Connect the client to the server          | connect                   |
        | subscribe       | Subscribe to a topic                      | subscribe test            |
        | unsubscribe     | Unsubscribe from a topic                  | unsubscribe test          |
        | publish         | Publish to a topic with data as bytes     | publish test 1 255        |
        | disconnect      | Disconnect from the server and exit       | disconnect                |
        | config          | Configure the connection to the broker    | config 127.0.0.1 65432    |
        | open            | Open a new TCP connection                 | open                      |
        | send            | Send a raw TCP packet                     | send 1 0                  |
        | close           | Close the TCP connection                  | close                     |
        | sniffer         | Start or stop the sniffer manually        | sniffer start | stop      |
        | help            | Display the current help screen           | help or help <cmd>        |
        | quit            | Stop all threads and quit the program     | quit                      |
        -------------------------------------------------------------------------------------------
        """

        if command:
            help_string = getattr(self, command).__doc__

        self.show(help_string)

    ###############################################################################################

    def sniffer(self, argument=""):
        """ Start a sniffer thread to capture packets from the broker """

        try:
            # Activate the new listener
            if argument == "start":

                # Clear a terminate signal left set by a previous stop/disconnect,
                # otherwise the new listener would exit immediately and the
                # reconnected client would silently receive nothing.
                self.terminate.clear()

                self.listener = threading.Thread(target=self.listen, args=[self.terminate, ])
                self.listener.daemon = True
                self.listener.start()

            # Stop the active listener
            elif argument == "stop":
                self.terminate.set()
                self.listener.join()

            # Show the current state of the listener
            else:
                if self.listener:
                    self.show("Listener is {0}".format(self.listener.is_alive()))
                else:
                    self.show("Listener not started yet")

        except BaseException as e:
            self.show(e)

    ###############################################################################################

    def connect(self, payload=""):
        """ Open a TCP connection, start the listener and send a CONNECT packet to the broker """

        # Open a new TCP connection
        self.open()

        # Start the listener
        self.sniffer(argument="start")

        # Send a CONNECT packet to the broker
        super(HvlpClientApp, self).connect(payload)

    ###############################################################################################

    def disconnect(self, payload=""):
        """ Stop the listener, send a DISCONNECT packet and close the TCP connection """

        # Stop listener
        if self.listener:
            self.sniffer(argument="stop")

        # Send `DISCONNECT` packet to the broker
        super(HvlpClientApp, self).disconnect(payload)

        # Close the TCP connection
        self.close()

    ###############################################################################################

    def on_packet(self, packet):
        """ Activated when a packet is received """

        # Call the parent handler
        super(HvlpClientApp, self).on_packet(packet)

        # Print the command prompt
        self.show(self.PROMPT, end='')

    ###############################################################################################

    def run(self):
        """" Client application activity """

        print("HVLP Client started ...")

        try:

            while True:

                try:
                    # Parse the command line
                    command, args = self.wait()

                    # Execute the desired action
                    if command == 'connect':
                        self.connect(*args)

                    elif command == 'subscribe':
                        self.subscribe(*args)

                    elif command == 'unsubscribe':
                        self.unsubscribe(*args)

                    elif command == 'publish':
                        self.publish(*args)

                    elif command == 'disconnect':
                        self.disconnect(*args)

                    elif command == 'config':
                        self.config(*args)

                    elif command == 'open':
                        self.open()

                    elif command == 'close':
                        self.close()

                    elif command == 'sniffer':
                        self.sniffer(*args)

                    elif command == 'send':
                        self.send(*args)

                    elif command == 'help':
                        self.help(*args)

                    elif command == 'quit':
                        self.quit()
                        break

                    else:
                        raise HvlpCommandError

                # Expected errors
                except HvlpError as e:
                    self.show(e)

        # User interrupt
        except KeyboardInterrupt:
            self.stop()

        # Unknown exceptions
        except Exception as e:
            self.show(e)

        # Cleanup
        finally:
            self.stop()
            self.show("Client exit...")


###################################################################################################
# MAIN
###################################################################################################

def run_client(server_addr='localhost', port=65432):
    """ Wrapper function for an easy start of the client application """

    # TODO: Enhance broker to allow configuration form the command line or configuration file
    configure_logger(level=logging.DEBUG)
    client = HvlpClientApp(srv_addr=server_addr, port=int(port))
    client.run()


if __name__ == "__main__":
    run_client(*sys.argv[1:])
