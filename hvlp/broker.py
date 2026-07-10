# coding: utf-8
from __future__ import print_function
from __future__ import unicode_literals

from hvlp.session import *
from hvlp.defines import *
from hvlp.register import *
from hvlp.logger import *

import socket
import threading


class HvlpBroker(threading.Thread):
    """Hilscher Variable Length Protocol Broker

    The broker is a custom thread, which spawns session threads on incoming TCP connections. The
    broker class offers tools for topic and client registration.

    Args:
        ip_addr         : Broker IP address
        port            : Broker port
        session         : Session class used to handle incoming messages

    Attributes:
        ip_addr         : Broker IP address
        port            : Broker Port
        session         : Session class used to handle incoming messages
        srv_sock        : Server socket waiting on IP:PORT for new connections
        register        : Register object with thread-safe access to store shared data
        log             : Logging object
        terminate       : Signal to stop the broker execution

    Examples:

        # Run as an application (handles keyboard interrupts)
        broker = HvlpBroker()
        broker.run()

        # Run as a thread (e.g. spawn multiple brokers for load-balancers)
        broker = HvlpBroker()
        broker.start()
        ...
        broker.stop()

    """

    def __init__(
            self,
            ip_addr=DEFAULT_IP,
            port=DEFAULT_PORT,
            session=HvlpSession
    ):

        super(HvlpBroker, self).__init__()

        # Network parameters
        self.ip_addr = ip_addr
        self.port = port
        self.session = session
        self.srv_sock = socket.socket()

        # Register services
        self.register = HvlpBrokerRegister()

        # Logging configuration
        self.log = logging.getLogger(self.__class__.__name__)
        self.log.addHandler(logging.NullHandler())

        # Stop signal
        self.terminate = threading.Event()

    def stop(self):
        """ Stop the broker thread """

        # Stop all sessions
        for session in self.register.get_sessions():
            session.stop()

        # Terminate the broker
        self.terminate.set()

    def run(self):
        """ Broker activity method

         While stop signal not active:
             1. Create a server socket on IP:PORT
             2. Listen for connection requests from clients
             3. On connection request create a new session
             4. Start the new session to handle incoming messages

         """

        try:

            self.log.info("Broker started...")

            # Allow the port to be re-bound immediately after a restart instead
            # of failing with EADDRINUSE while the old socket lingers.
            self.srv_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.srv_sock.bind((self.ip_addr, self.port))
            self.srv_sock.listen(5)

            # Configure the socket on the server side to work in non-blocking mode
            # This allows the accept method in the broker loop to time-out and poll periodically
            # for the occurence of the stop condition.
            self.srv_sock.settimeout(1)

            while not self.terminate.is_set():

                try:

                    # Wait for new connection requests
                    client, addr_info = self.srv_sock.accept()
                    self.log.debug("Connection from: {0}".format(addr_info))

                    # Start the session thread
                    name = "{IP}:{PORT}".format(IP=addr_info[0], PORT=str(addr_info[1]))
                    session = self.session(
                        client=client,
                        register=self.register,
                    )

                    t = threading.Thread(target=session.run, name=name)
                    t.daemon = True
                    t.start()

                # The accept timed-out
                except socket.timeout:
                    pass

        # Stopped by the user
        except KeyboardInterrupt:
            self.stop()

        # Cleanup on unexpected exceptions
        finally:
            self.stop()

            # Release the listening socket so the port is freed on shutdown.
            try:
                self.srv_sock.close()
            except socket.error:
                pass

            self.log.info("Broker exit...")


####################################################################################################
# MAIN
####################################################################################################

def run_broker(ip='localhost', port=65432):
    """ Wrapper function for the broker """

    # TODO: Enhance broker to allow configuration form the command line or configuration file
    configure_logger(level=logging.DEBUG)
    broker = HvlpBroker(
        ip_addr=ip,
        port=int(port),
        session=HvlpSession,
    )

    broker.run()


if __name__ == "__main__":
    run_broker(*sys.argv[1:])
