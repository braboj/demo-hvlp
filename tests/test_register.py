# coding: utf-8
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

from hvlp.register import HvlpBrokerRegister
from hvlp.logger import *


###################################################################################################
# MODULE TESTS
###################################################################################################

def test_reset(register):

    test_subscribe(register)
    test_append(register)

    register.reset()
    logging.info("After reset the register is {0}".format(register))

    assert (register == {} and register.get_sessions() == set())


def test_subscribe(register, session='test'):

    register.add_session(session)
    sessions = register.get_sessions()
    logging.info("After subscribe the registered sessions are `{0}`".format(session))

    assert (session in sessions)
    register.reset()


def test_unsubscribe(register, session='test'):

    register.add_session(session)
    sessions = register.get_sessions()
    logging.info("After subscribe the registered sessions are `{0}`".format(sessions))

    register.remove_session(session)
    sessions = register.get_sessions()
    logging.info("After unsubscribe the registered sessions are {0}".format(sessions))

    assert (sessions == set())
    register.reset()


def test_append(register):

    topics = ['test1', 'test2']
    client = 'test_client'

    register.subscribe(topics, client)
    subscriptions = register.get_topics(client)

    logging.info("The client `{0}` is now subscribed to {1}".format(client, subscriptions))

    assert (subscriptions == set(topics))
    register.reset()


def test_remove(register):

    topics = ['test1', 'test2', 'test3']
    client = 'test_client'

    register.subscribe(topics, client)
    register.unsubscribe(topics, client)
    subscriptions = register.get_topics(client)

    logging.info("The client `{0}` is now subscribed to {1}".format(client, subscriptions))

    assert (subscriptions == set())
    register.reset()


def test_get_subscribers(register):

    topics = ['test1', 'test2']
    client = 'test_client'

    register.subscribe(topics, client)

    for topic in topics:
        subscribers = register.get_subscribers(topic)
        logging.info("The topic `{0}` has the the subscribers {1}".format(topic, subscribers))
        assert (client in subscribers)

    register.reset()


def test_get_topics(register):

    topics = ['test1', 'test2']
    client = 'test_client'

    register.subscribe(topics, client)
    subscriptions = register.get_topics(client)
    logging.info("The client  `{0}` is subscribed to {1}".format(client, subscriptions))

    assert (subscriptions == set(topics))
    register.reset()


def test_get_sessions(register):

    test_session = 'test session'

    logging.info("Notify the register with session name `{0}`".format(test_session))
    register.add_session(session=test_session)

    sessions = register.get_sessions()
    logging.info("The register has the following sessions: {0}".format(sessions))

    assert (test_session in sessions)
    register.reset()


###################################################################################################
# MAIN
###################################################################################################

if __name__ == "__main__":

    configure_logger()

    r = HvlpBrokerRegister()
    test_reset(r)
    test_subscribe(r)
    test_unsubscribe(r)
    test_append(r)
    test_remove(r)
    test_get_subscribers(r)
    test_get_topics(r)
    test_get_sessions(r)
