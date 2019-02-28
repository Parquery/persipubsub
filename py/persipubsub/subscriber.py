#!/usr/bin/env python3
"""Receive messages persistently from the queue."""

import contextlib
import datetime
import pathlib
import time
from typing import Any, Iterator, Optional, Union

import icontract
import lmdb  # pylint: disable=unused-import

import persipubsub.database
import persipubsub.queue

# pylint: disable=protected-access


def initialize(path: pathlib.Path, env: lmdb.Environment,
               identifier: str) -> 'Subscriber':
    """
    Initialize a new subscriber.

    :param path: to LMDB
    :param env: open LMDB environment
    :param identifier: of the subscriber
    :return: Subscriber to receive messages
    """
    subscriber = Subscriber()
    subscriber.init(identifier=identifier, path=path, env=env)
    return subscriber


class Subscriber:
    """
    Handle receiving messages stored in the queue.

    :ivar identifier: subscriber ID
    :vartype pub_id: str
    :ivar queue: from which messages are received
    :vartype queue: persipubsub.queue.Queue
    """

    def __init__(self) -> None:
        """Initialize class object."""
        self.identifier = None  # type: Optional[str]
        self.queue = None  # type: Optional[persipubsub.queue._Queue]
        self.closed = False

    def init(self,
             identifier: str,
             path: Union[pathlib.Path, str],
             env: Optional[lmdb.Environment] = None) -> None:
        """
        Initialize.

        :param identifier: unique subscriber id
        :param path: path to the queue
        :param env: open LMDBenvironment
        """
        self.identifier = identifier
        assert self.identifier is not None
        self.queue = persipubsub.queue._Queue()  # pylint: disable=protected-access
        self.queue.init(path=path, env=env)
        assert self.queue is not None

    def __enter__(self) -> 'Subscriber':
        """Enter the context and give the sub prepared in the constructor."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Leave the context."""
        self.close()

    def close(self) -> None:
        """Close subscriber."""
        self.closed = True

    @icontract.require(lambda timeout: timeout > 0)
    @icontract.require(lambda retries: retries > 0)
    @icontract.require(lambda self: not self.closed)
    @contextlib.contextmanager
    def receive(self, timeout: int = 60,
                retries: int = 10) -> Iterator[Optional[bytes]]:
        """
        Receive messages from the queue.

        :param timeout:
            time waiting for a message. If none arrived until the timeout then
            None will be returned. (secs)
        :param retries: number of tries to check if a msg arrived in the queue
        :return:
            Iterator because of decorator which contains a message in bytes
        """
        msg = None
        msg_id = None
        end = int(datetime.datetime.utcnow().timestamp()) + timeout
        assert self.queue is not None
        assert self.identifier is not None
        try:
            while int(datetime.datetime.utcnow().timestamp()) <= end:
                msg_id, msg = self.queue.front(sub_id=self.identifier)
                if msg is not None:
                    break
                time.sleep(timeout / retries)
            yield msg
        finally:
            pass

        if msg_id is not None:
            self._pop(msg_id=msg_id)

    def _pop(self, msg_id: bytes) -> None:
        """Pop a message from the subscriber's database."""
        assert self.queue is not None
        assert self.identifier is not None
        self.queue.pop(sub_id=self.identifier, msg_id=msg_id)

    @icontract.require(lambda timeout: timeout > 0)
    @icontract.require(lambda retries: retries > 0)
    @icontract.require(lambda self: not self.closed)
    @contextlib.contextmanager
    def receive_to_top(self, timeout: int = 60,
                       retries: int = 10) -> Iterator[Optional[bytes]]:
        """
        Pops all messages until the most recent one and receive the latest.

        The method can be used in the case when a particular subscriber cares
        only about the very last message. The messages are not popped for other
        subscribers.

        If you want to store only the latest message for all subscribers, then
        use high water mark max_messages = 1.

        :param timeout:
            time waiting for a message. If none arrived until the timeout then
            None will be returned. (secs)
        :param retries: number of tries to check if a msg arrived in the queue
        :return:
            Iterator because of decorator which contains a message in bytes
        """
        assert self.queue is not None
        assert self.queue.env is not None
        assert self.identifier is not None
        with self.queue.env.begin(write=False) as txn:
            sub_db = self.queue.env.open_db(
                key=persipubsub.database.str_to_bytes(self.identifier),
                txn=txn,
                create=False)
            sub_stat = txn.stat(db=sub_db)
            # pop all message except the most recent one
            msg_to_pop_num = sub_stat['entries'] - 1

        for _ in range(msg_to_pop_num):
            self.queue.pop(sub_id=self.identifier)

        msg = None
        msg_id = None
        end = int(datetime.datetime.utcnow().timestamp()) + timeout
        try:
            while int(datetime.datetime.utcnow().timestamp()) <= end:
                msg_id, msg = self.queue.front(sub_id=self.identifier)
                if msg is not None:
                    break
                time.sleep(timeout / retries)
            yield msg
        finally:
            pass

        if msg_id is not None:
            self._pop(msg_id=msg_id)
