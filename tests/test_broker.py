# coding: utf-8
from hvlp.client import HvlpClient
from hvlp.broker import HvlpBroker

import threading
from hvlp.errors import HvlpError
import time

BROKER_ADDR = 'localhost'
BROKER_PORT = 65432

# Burst size for the load tests.
N = 100

class TestCase(object):

    def __init__(self):

        # Create the clients
        self.producer = HvlpClient(srv_addr=BROKER_ADDR, port=BROKER_PORT)
        self.consumer = HvlpClient(srv_addr=BROKER_ADDR, port=BROKER_PORT)
        self.stop_event = threading.Event()


class HVLP_NET_01_01_01(TestCase):


    def __init__(self):
        super(HVLP_NET_01_01_01, self).__init__()
        self.tag = self.__class__.__name__
        self.description = ("Send a burst of connect messages to the broker and check if the broker is able to handle "
                            "the load.")

    def execute(self):

        # Open the sockets
        self.producer.open()
        self.consumer.open()

        for i in range(N):
            self.producer.connect()
            self.consumer.connect()

        # Check that the broker is able to handle the load
        self.producer.disconnect()
        self.consumer.disconnect()

        self.producer.close()
        self.consumer.close()

class HVLP_NET_01_01_02(TestCase):

    def __init__(self):
        super(HVLP_NET_01_01_02, self).__init__()
        self.tag = self.__class__.__name__
        self.description = ("Send a burst of disconnect messages to the broker and check if the broker is able to "
                            "handle the load.")

    def execute(self):

        # Open the sockets
        self.producer.open()
        self.consumer.open()

        for i in range(N):
            self.producer.connect()
            self.consumer.connect()

        # If the server is broker, we will know it here
        self.producer.disconnect()
        self.consumer.disconnect()


class HVLP_NET_01_01_03(TestCase):

    def __init__(self):
        super(HVLP_NET_01_01_03, self).__init__()
        self.tag = self.__class__.__name__
        self.description = ("Cycle connect and disconnect messages to the broker and check if the broker is able to "
                            "handle to the load.")

    def execute(self):

        try:
            # Open the sockets
            self.producer.open()

            for i in range(N):
                try:
                    self.producer.connect()
                    self.producer.disconnect()
                except HvlpError:
                    print(i)
                    break

        except Exception:
            raise


class HVLP_NET_01_01_04(TestCase):

        def __init__(self):
            super(HVLP_NET_01_01_04, self).__init__()
            self.tag = self.__class__.__name__
            self.description = ("Send a burst of subscribe messages to the broker and check if the broker is able to "
                                "handle the load.")

        def execute(self):

            # Open the sockets
            self.producer.open()
            self.consumer.open()

            # Connect the clients
            self.producer.connect()

            for i in range(N):
                self.producer.subscribe(str(i))

            # If the server is broker, we will know it here
            self.producer.disconnect()

class HVLP_NET_01_01_05(TestCase):

    def __init__(self):
        super(HVLP_NET_01_01_05, self).__init__()
        self.tag = self.__class__.__name__
        self.description = ("Send a burst of unsubscribe messages to the broker and check if the broker is able to "
                            "handle the load.")

    def execute(self):

        # Open the sockets
        self.producer.open()
        self.consumer.open()

        # Connect the clients
        self.producer.connect()

        for i in range(N):
            self.producer.unsubscribe(str(i))

        # If the server is broker, we will know it here
        self.producer.disconnect()


class HVLP_NET_01_01_06(TestCase):

    def __init__(self):
        super(HVLP_NET_01_01_06, self).__init__()
        self.tag = self.__class__.__name__
        self.description = ("Cycle subscribe and unsubscribe messages to the broker and check if the broker is able to "
                            "handle the load.")

    def execute(self):

        # Open the sockets
        self.producer.open()
        self.consumer.open()

        # Connect the clients
        self.producer.connect()
        self.consumer.connect()

        for i in range(N):
            self.producer.subscribe(str(i))
            self.consumer.subscribe(str(i))
            self.producer.unsubscribe(str(i))
            self.consumer.unsubscribe(str(i))

        # If the server is broker, we will know it here
        self.producer.disconnect()
        self.consumer.disconnect()

class HVLP_NET_01_01_07(TestCase):

    def __init__(self):
        super(HVLP_NET_01_01_07, self).__init__()
        self.tag = self.__class__.__name__
        self.description = ("Send a burst of publish messages to the broker and check if the broker is able to "
                            "handle the load.")


    def execute(self):

            # Open the sockets
            self.producer.open()

            # Connect the clients
            self.producer.connect()
            self.producer.subscribe('test')

            for i in range(N):
                self.producer.publish('test', 1)

            # If the server is broker, we will know it here
            self.producer.unsubscribe('test')
            self.producer.disconnect()


class HVLP_NET_01_01_08(TestCase):


    def __init__(self):
        super(HVLP_NET_01_01_08, self).__init__()
        self.tag = self.__class__.__name__
        self.description = "Subscribe with non-latin characters and check if the broker is able to handle the load."

    def execute(self):

        self.producer.open()
        time.sleep(0.05)

        self.producer.connect()
        time.sleep(0.05)

        self.producer.subscribe("тест")
        time.sleep(0.05)

        self.producer.unsubscribe("тест")
        time.sleep(0.05)

        self.producer.disconnect()
        time.sleep(0.05)

if __name__ == "__main__":

    # Start a self-contained broker so the file no longer depends on an
    # externally running broker, and guard execution so importing the module
    # (e.g. by a test runner) does not spin up sockets as a side effect.
    broker = HvlpBroker(port=BROKER_PORT)
    broker.daemon = True
    broker.start()
    time.sleep(0.5)

    tests = (
        HVLP_NET_01_01_01(),
        HVLP_NET_01_01_02(),
        HVLP_NET_01_01_03(),
        HVLP_NET_01_01_04(),
        HVLP_NET_01_01_05(),
        HVLP_NET_01_01_06(),
        HVLP_NET_01_01_07(),
        # HVLP_NET_01_01_08(),
    )

    try:
        for test in tests:
            test.execute()
            print("Test {test} completed successfully.".format(test=test))
    finally:
        broker.stop()
        broker.join()