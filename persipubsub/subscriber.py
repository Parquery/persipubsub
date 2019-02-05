#!/usr/bin/env python3
"""Receive messages persistent from publisher to subscriber."""

import contextlib
import datetime
import pathlib
import time
from typing import Generator, Optional, Union

import icontract
import lmdb  # pylint: disable=unused-import

import persipubsub.queue

# pylint: disable=protected-access


class Subscriber:
    """
    Create Subscriber ready to receive messages.

    :ivar sub_id: subscriber ID
    :vartype pub_id: str
    :ivar queue: from which messages are received
    :vartype queue: persipubsub.queue.Queue
    """

    def __init__(self) -> None:
        """Initialize class object."""
        self.sub_id = None  # type: Optional[str]
        self.queue = None  # type: Optional[persipubsub.queue._Queue]

    def init(self, sub_id: str, path: Union[pathlib.Path, str]) -> None:
        """
        Initialize.

        :param sub_id: unique subscriber id
        :param path: path to the queue
        """
        self.sub_id = sub_id
        assert isinstance(self.sub_id, str)
        self.queue = persipubsub.queue._Queue()  # pylint: disable=protected-access
        self.queue.init(path=path)
        assert isinstance(self.queue, persipubsub.queue._Queue)

    def __enter__(self) -> 'Subscriber':
        """Enter the context and give the sub prepared in the constructor."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Leave the context."""

    @icontract.require(lambda timeout: timeout > 0)
    @icontract.require(lambda retries: retries > 0)
    @contextlib.contextmanager
    def receive(self, timeout: int = 60, retries: int = 10) -> Generator:
        """
        Receive messages from the publisher.

        :param timeout: time waiting for a message. If none arrived until the
            timeout then None will be returned. (secs)
        :param retries: number of tries to check if a msg arrived in the queue
        """
        msg = None
        end = int(datetime.datetime.utcnow().timestamp()) + timeout
        assert isinstance(self.queue, persipubsub.queue._Queue)
        assert isinstance(self.sub_id, str)
        try:
            while int(datetime.datetime.utcnow().timestamp()) <= end:
                msg = self.queue.front(sub_id=self.sub_id)
                if msg is not None:
                    break
                time.sleep(timeout / retries)
            yield msg
        finally:
            pass

        if msg is not None:
            self._pop()

    def _pop(self) -> None:
        """Pop a message from the subscriber's lmdb."""
        assert isinstance(self.queue, persipubsub.queue._Queue)
        assert isinstance(self.sub_id, str)
        self.queue.pop(sub_id=self.sub_id)

    @icontract.require(lambda timeout: timeout > 0)
    @icontract.require(lambda retries: retries > 0)
    @contextlib.contextmanager
    def receive_to_top(self, timeout: int = 60, retries: int = 10) -> Generator:
        """
        Pops all messages until the most recent one and receive the latest.

        Used in the case that a particular subscriber cares only about the very
        last message and other subscribers care about all the messages in the
        queue.
        For another use case, when you only want to store the latest message
        and all subscribers are interested only in the latest, then use
        high water mark max_msgs_num = 1.

        :param timeout: time waiting for a message. If none arrived until the
            timeout then None will be returned. (secs)
        :param retries: number of tries to check if a msg arrived in the queue
        """
        assert isinstance(self.queue, persipubsub.queue._Queue)
        assert isinstance(self.queue.env, lmdb.Environment)
        assert isinstance(self.sub_id, str)
        with self.queue.env.begin(write=False) as txn:
            sub_db = self.queue.env.open_db(
                key=persipubsub.encoding(self.sub_id), txn=txn, create=False)
            sub_stat = txn.stat(db=sub_db)
            # pop all message except the most recent one
            msg_to_pop_num = sub_stat['entries'] - 1

        for _ in range(msg_to_pop_num):
            self.queue.pop(sub_id=self.sub_id)

        msg = None
        end = int(datetime.datetime.utcnow().timestamp()) + timeout
        assert isinstance(self.queue, persipubsub.queue._Queue)
        assert isinstance(self.sub_id, str)
        try:
            while int(datetime.datetime.utcnow().timestamp()) <= end:
                msg = self.queue.front(sub_id=self.sub_id)
                if msg is not None:
                    break
                time.sleep(timeout / retries)
            yield msg
        finally:
            pass

        if msg is not None:
            self._pop()
