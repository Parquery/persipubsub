#!/usr/bin/env python3
"""Fabricate new persipubsub components."""

import pathlib

import persipubsub.control
import persipubsub.publisher
import persipubsub.subscriber


class Environment:
    """Fabricate persipubsub components."""

    def __init__(self, path: pathlib.Path):
        """Initialize."""
        self.path = path

    def __enter__(self) -> 'Environment':
        """Enter the context and give environment prepared to constructor."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Leave the context."""

    def new_control(self) -> persipubsub.control.Control:
        """
        Fabricate a new control.

        :return: Control to initialize queues
        """
        control = persipubsub.control.Control(path=self.path)
        control.init()
        return control

    def new_publisher(
            self, autosync: bool = False) -> persipubsub.publisher.Publisher:
        """
        Fabricate a new publisher.

        :return: Publisher to send messages
        """
        publisher = persipubsub.publisher.Publisher()
        publisher.init(path=self.path, autosync=autosync)
        return publisher

    def new_subscriber(self,
                       identifier: str) -> persipubsub.subscriber.Subscriber:
        """
        Fabricate a new subscriber.

        :param identifier: of the subscriber
        :return: Subscriber to receive messages
        """
        subscriber = persipubsub.subscriber.Subscriber()
        subscriber.init(sub_id=identifier, path=self.path)
        return subscriber


def new_environment(path: pathlib.Path) -> Environment:
    """
    Fabricate a new environment.

    :param path: path to the queue
    :return Environment to create control, publisher and subscriber
    """
    return Environment(path=path)
