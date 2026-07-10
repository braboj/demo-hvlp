# coding: utf-8
from __future__ import print_function
from __future__ import unicode_literals

import logging
import threading


class HvlpBrokerRegister(dict):
    """ Thread-safe register class for the HVLP broker optimized for performance.

    This class keeps track of connections, topics, and the clients subscribed to them.
    It's optimized for minimal lock contention and efficient data access.

    """

    def __init__(self):
        super(HvlpBrokerRegister, self).__init__()

        # A reentrant lock to allow recursive locking in the same thread
        self.__lock = threading.RLock()

        # Logger for debugging and info messages
        self.__log = logging.getLogger(self.__class__.__name__)

        # Using a set for more efficient add/remove operations
        self.__sessions = set()

    def reset(self):
        """ Reset the register object, clearing sessions and topics."""

        # Ensure thread safety through lock
        with self.__lock:
            self.__sessions.clear()
            self.clear()

    def add_session(self, session):
        """ Add a session to the register.

        Args:
            session: The session object to be added.

        """

        # Lock to ensure thread-safe modification
        with self.__lock:
            self.__sessions.add(session)

    def remove_session(self, session):
        """ Remove a session from the register.

        Args:
            session: The session object to be removed.

        """

        # Lock to ensure thread-safe modification
        with self.__lock:

            # Discard session without raising an error if not found
            self.__sessions.discard(session)

    def subscribe(self, topics, client):
        """ Subscribe a client to a set of topics.

        Args:
            topics: A list of topic names to subscribe the client to.
            client: The client object subscribing to the topics.

        """

        # Temporary storage for updates to minimize locked time
        updates = {}

        # Lock to ensure thread-safe read and update
        with self.__lock:
            for topic in topics:

                # Initialize topic if not present
                if topic not in self:
                    self[topic] = set()

                # Add client to topic if not already subscribed
                if client not in self[topic]:

                    # Union operation to add client
                    updates[topic] = self[topic] | {client}

            # Batch update to reduce lock contention
            self.update(updates)

    def unsubscribe(self, topics, client):
        """ Unsubscribe a client from a set of topics.

        Args:
            topics: A list of topic names to unsubscribe the client from.
            client: The client object being unsubscribed.

        """

        # Ensure thread-safe operation
        with self.__lock:

            # Iterate over topics to unsubscribe from
            for topic in topics:

                # Check if topic is present
                if topic in self:

                    # Safely remove client
                    self[topic].discard(client)

                    # If no subscribers left, delete the topic
                    if not self[topic]:
                        del self[topic]

    def get_subscribers(self, topic):
        """ Get subscribers to a specific topic.

        Args:
            topic: The topic name whose subscribers are to be retrieved.

        Returns:
            A set of subscribers to the given topic.

        """

        # Ensure thread-safe access
        with self.__lock:

            # Return a copy so callers can iterate without racing a concurrent
            # subscribe/unsubscribe that mutates the internal set.
            return set(self.get(topic, set()))

    def get_topics(self, client):
        """ Get all topics a specific client is subscribed to.

        Args:
            client: The client object whose subscribed topics are to be retrieved.

        Returns:
            A set of topic names the client is subscribed to.
        """

        # Lock to ensure thread-safe iteration
        with self.__lock:

            # Use set comprehension for efficient construction
            return {topic for topic, subscribers in self.items() if client in subscribers}

    def get_sessions(self):
        """ Get all registered sessions.

        Returns:
            A set of all session objects registered in the broker.

        """

        # Ensure thread-safe access
        with self.__lock:

            # Return a copy so callers (e.g. broker shutdown) can iterate while
            # sessions remove themselves without "set changed size" errors.
            return set(self.__sessions)
