# coding: utf-8
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

from hvlp.broker import *
from hvlp.client import *
from hvlp.logger import *

import time


def _first_session(register, timeout=2.0):
    """ Return the single registered session, waiting briefly for it to appear.

    `get_sessions()` returns a `set`, which is unordered and not subscriptable,
    so the previous `[-1]` indexing no longer works. Each test connects exactly
    one client, so the single member of the set is the session under test.

    """

    deadline = time.time() + timeout
    while time.time() < deadline:
        sessions = register.get_sessions()
        if sessions:
            return next(iter(sessions))
        time.sleep(0.05)

    raise AssertionError("No session was registered within {0}s".format(timeout))


def test_connect():

    broker = HvlpBroker(port=65000)
    broker.daemon = True
    broker.start()
    register = broker.register

    try:
        # Perform the TCP handshake
        client = HvlpClient(port=65000)
        client.open()
        time.sleep(1)

        # Get the (only) registered session
        session = _first_session(register)

        # Check the session state after the TCP handshake.
        # HvlpSession is not a Thread (the broker runs `session.run` in a
        # separate thread), so "alive" is read from the session's own stop
        # signal instead of Thread.is_alive().
        assert (session.state == HvlpSession.WAIT_CONNECT and not session.terminate.is_set())
        print(session.state)

        # Send the CONNECT packet to the broker
        client.connect()
        time.sleep(1)

        # Check the session state after the CONNECT packet
        assert (session.state == HvlpSession.CONNECTED and not session.terminate.is_set())
        print(session.CONNECTED)

    finally:
        broker.stop()
        broker.join()


def test_disconnect():

    broker = HvlpBroker(port=65001)
    broker.daemon = True
    broker.start()
    register = broker.register

    try:
        # Perform the TCP handshake
        client = HvlpClient(port=65001)

        # Open the connection
        client.open()
        time.sleep(1)

        # Get the (only) registered session
        session = _first_session(register)

        # Send a `connect` packet to the broker
        client.connect()
        time.sleep(1)

        # Send a disconnect packet to the broker
        client.disconnect()
        time.sleep(1)

        # The session must have stopped after disconnect
        assert (session.terminate.is_set())

    finally:
        broker.stop()
        broker.join()


def test_subscribe():

    broker = HvlpBroker(port=65002)
    broker.daemon = True
    broker.start()
    register = broker.register

    try:
        # Perform the TCP handshake
        client = HvlpClient(port=65002)

        # Open the connection
        client.open()
        time.sleep(1)

        # Get the (only) registered session
        session = _first_session(register)

        # Send a `connect` packet to the broker
        client.connect()
        time.sleep(1)

        # Send a `subscribe` packet to the broker
        client.subscribe('test')
        time.sleep(1)

        topics = register.get_topics(client=session.client)
        assert ('test' in topics)

        # Send a disconnect packet to the broker
        client.disconnect()
        time.sleep(1)

    finally:
        broker.stop()
        broker.join()


###################################################################################################
# MAIN
###################################################################################################

if __name__ == "__main__":
    configure_logger()
    test_connect()
    test_disconnect()
    test_subscribe()
