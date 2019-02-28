#!/usr/bin/env python3
"""Create new persipubsub components."""

import pathlib
from typing import Any, Optional, Set

import icontract

import persipubsub.control
import persipubsub.database
import persipubsub.publisher
import persipubsub.queue
import persipubsub.subscriber

# pylint: disable = protected-access


class Environment:
    """
    Provide an environment which produces persipubsub components.

    Please note that the environment is a context manager and needs to be
    properly closed.

    Only one environment of each queue per process is allowed and it's
    forbidden to fork the environment or any child components to multiple
    processes.
    In that case, persipubsub is multi-threading and multi-process safe.
    If multiple environments of the same queue are active on the same process,
    or the environment is forked to multiple processes the underlying LMDB
    write lock is broken and results in undefined behaviour.

    :ivar path: to the queue
    :vartype path: pathlib.Path
    """

    def __init__(self, path: pathlib.Path) -> None:
        """
        Initialize.

        :param path: to the queue
        """
        self.path = path
        self._env = persipubsub.queue._initialize_environment(
            queue_dir=self.path,
            max_reader_num=persipubsub.database.DEFAULT_MAX_READERS,
            max_db_num=persipubsub.database.DEFAULT_MAX_DB_NUM,
            max_db_size_bytes=persipubsub.database.DEFAULT_MAX_DB_SIZE)
        self.closed = False

    def __enter__(self) -> 'Environment':
        """Enter the context and give environment prepared to constructor."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Leave the context."""
        self.close()

    def close(self) -> None:
        """
        Close LMDB environment and persipubsub environment.

        When persipubsub environment is closed any control, publisher,
        subscriber is invalid. Repeat calls to close() have no effect.

        [https://lmdb.readthedocs.io/en/release/]
        Close the environment, invalidating any open iterators, cursors, and
        transactions. Repeat calls to close() have no effect.

        Explicit call might be not necessary. See the latest example of the
        creator of py-lmdb.
        https://github.com/dw/py-lmdb/blob/master/examples/address-book.py
        :return:
        """
        self._env.close()
        self.closed = True

    @icontract.require(lambda self: not self.closed)
    def new_control(self,
                    subscriber_ids: Optional[Set[str]] = None,
                    high_water_mark: persipubsub.queue.
                    HighWaterMark = persipubsub.queue.HighWaterMark(),
                    strategy: persipubsub.queue.Strategy = persipubsub.queue.
                    Strategy.PRUNE_FIRST) -> persipubsub.control.Control:
        """
        Create a new control.

        :param subscriber_ids: subscribers of the queue
        :param high_water_mark: high water mark limit of the queue
        :param strategy: used to prune queue
        :return: Control to create and maintain queue
        """
        # pylint: disable=too-many-arguments
        return persipubsub.control.initialize(
            path=self.path,
            env=self._env,
            subscriber_ids=subscriber_ids,
            high_water_mark=high_water_mark,
            strategy=strategy)

    @icontract.require(lambda self: not self.closed)
    def new_publisher(
            self, autosync: bool = False) -> persipubsub.publisher.Publisher:
        """
        Create a new publisher.

        :param autosync: if True, store data automatically in LMDB
        :return: Publisher to send messages
        """
        return persipubsub.publisher.initialize(
            path=self.path, autosync=autosync, env=self._env)

    @icontract.require(lambda self: not self.closed)
    def new_subscriber(self,
                       identifier: str) -> persipubsub.subscriber.Subscriber:
        """
        Create a new subscriber.

        :param identifier: of the subscriber
        :return: Subscriber to receive messages
        """
        return persipubsub.subscriber.initialize(
            path=self.path, env=self._env, identifier=identifier)


def initialize(path: pathlib.Path) -> Environment:
    """
    Create a new environment.

    :param path: path to the queue
    :return: Environment to create control, publisher and subscriber
    """
    return Environment(path=path)
