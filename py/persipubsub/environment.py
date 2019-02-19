#!/usr/bin/env python3
"""Create new persipubsub components."""

import pathlib
from typing import Any, Optional, Sequence

import persipubsub.control
import persipubsub.publisher
import persipubsub.queue
import persipubsub.subscriber

# pylint: disable = protected-access


class Environment:
    """
    Create persipubsub components.

    Only one environment of each queue per process allowed and it's forbidden to
    fork environment or any child components to multiple processes.
    In that case, persipubsub is multi-threading and multi-process safe.
    If multiple environments of the same queue are active on the same process,
    or environment is forked to multiple processes the lock is broken and
    correctness can't be guaranteed.

    :ivar path: to the queue
    :vartype path: pathlib.Path
    :ivar env: only open lmdb environment of process
    :vartype env: lmdb.Environment
    """

    def __init__(self, path: pathlib.Path) -> None:
        """
        Initialize.

        :param path: to the queue
        """
        self.path = path
        self.env = persipubsub.queue._initialize_environment(
            queue_dir=self.path,
            max_reader_num=persipubsub.MAX_READER_NUM,
            max_db_num=persipubsub.MAX_DB_NUM,
            max_db_size_bytes=persipubsub.MAX_DB_SIZE_BYTES)

    def __enter__(self) -> 'Environment':
        """Enter the context and give environment prepared to constructor."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Leave the context."""

    # pylint: disable=too-many-arguments
    def new_control(self,
                    subscriber_ids: Optional[Sequence[str]] = None,
                    high_watermark: persipubsub.queue.
                    HighWaterMark = persipubsub.queue.HighWaterMark(),
                    strategy: persipubsub.queue.Strategy = persipubsub.queue.
                    Strategy.prune_first) -> persipubsub.control.Control:
        """
        Create a new control.

        :param subscriber_ids: subscribers of the queue
        :param high_watermark: high water mark limit of the queue
        :param strategy: used to prune queue
        :return: Control to create and maintain queue
        """
        control = persipubsub.control.Control(path=self.path, env=self.env)
        control.init(
            subscriber_ids=subscriber_ids,
            high_watermark=high_watermark,
            strategy=strategy)
        return control

    def new_publisher(
            self, autosync: bool = False) -> persipubsub.publisher.Publisher:
        """
        Create a new publisher.

        :param autosync: if True, store data automatically in lmdb
        :return: Publisher to send messages
        """
        publisher = persipubsub.publisher.Publisher()
        publisher.init(path=self.path, autosync=autosync, env=self.env)
        return publisher

    def new_subscriber(self,
                       identifier: str) -> persipubsub.subscriber.Subscriber:
        """
        Create a new subscriber.

        :param identifier: of the subscriber
        :return: Subscriber to receive messages
        """
        subscriber = persipubsub.subscriber.Subscriber()
        subscriber.init(identifier=identifier, path=self.path, env=self.env)
        return subscriber


def new_environment(path: pathlib.Path) -> Environment:
    """
    Create a new environment.

    :param path: path to the queue
    :return: Environment to create control, publisher and subscriber
    """
    return Environment(path=path)
