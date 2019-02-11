#!/usr/bin/env python3
"""Fabricate new persipubsub components."""

import pathlib
from typing import Any, Optional, Sequence

import persipubsub.control
import persipubsub.publisher
import persipubsub.queue
import persipubsub.subscriber

# pylint: disable = protected-access


class Environment:
    """Fabricate persipubsub components."""

    def __init__(self, path: pathlib.Path) -> None:
        """
        Initialize.

        :param path: to the queue
        """
        self.path = path

    def __enter__(self) -> 'Environment':
        """Enter the context and give environment prepared to constructor."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Leave the context."""

    # pylint: disable=too-many-arguments
    def new_control(self,
                    subscriber_ids: Optional[Sequence[str]] = None,
                    max_readers: int = 1024,
                    max_size: int = 32 * 1024**3,
                    high_watermark: persipubsub.queue.
                    HighWaterMark = persipubsub.queue.HighWaterMark(),
                    strategy: persipubsub.queue.Strategy = persipubsub.queue.
                    Strategy.prune_first) -> persipubsub.control.Control:
        """
        Fabricate a new control.

        :param subscriber_ids: subscribers of the queue
        :param max_readers: max number of reader of the lmdb
        :param max_size: max size of the lmdb in bytes
        :param high_watermark: high water mark limit of the queue
        :param strategy: used to prune queue
        :return: Control to create and maintain queue
        """
        control = persipubsub.control.Control(path=self.path)
        control.init(
            subscriber_ids=subscriber_ids,
            max_readers=max_readers,
            max_size=max_size,
            high_watermark=high_watermark,
            strategy=strategy)
        return control

    def new_publisher(
            self, autosync: bool = False) -> persipubsub.publisher.Publisher:
        """
        Fabricate a new publisher.

        :param autosync: if True, store data automatically in lmdb
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
        subscriber.init(identifier=identifier, path=self.path)
        return subscriber


def new_environment(path: pathlib.Path) -> Environment:
    """
    Fabricate a new environment.

    :param path: path to the queue
    :return: Environment to create control, publisher and subscriber
    """
    return Environment(path=path)
