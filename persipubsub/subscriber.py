#!/usr/bin/env python
"""Receive messages persistent from publisher to subscriber."""

import contextlib
import datetime
import pathlib
import time
from typing import Generator, Union

import icontract

import persipubsub.queue


class Sub:
    """
    Create Subscriber ready to receive messages.

    :ivar sub_id: subscriber ID
    :vartype pub_id: str
    :ivar queue: from which messages are received
    :vartype queue: persipubsub.queue.Queue
    """

    sub_id = None  # type: str
    queue = None  # type: persipubsub.queue._Queue

    def __init__(self) -> None:
        """Initialize class object."""

    def init(self, sub_id: str, config_pth: Union[pathlib.Path, str]) -> None:
        """
        Initialize.

        :param sub_id: unique subscriber id
        :param config_pth: path to the JSON config file
        """
        self.sub_id = sub_id

        config = persipubsub.get_config(path=config_pth)
        subscriber = config[sub_id]
        queue_dir = subscriber["in_queue"]
        self.queue = persipubsub.queue._Queue()  # pylint: disable=protected-access
        self.queue.init(config_pth=config_pth, queue_dir=queue_dir)

    def __enter__(self) -> 'Sub':
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
        self.queue.pop(sub_id=self.sub_id)

    def pop_to_top(self) -> None:
        """
        Pops all messages until the most recent one.

        Used for slow processes to ensure realtime processing.
        """
        with self.queue.env.begin(write=False) as txn:
            sub_db = self.queue.env.open_db(
                key=persipubsub.encoding(self.sub_id), txn=txn, create=False)
            sub_stat = txn.stat(db=sub_db)
            # pop all message except the most recent one
            msg_to_pop_num = sub_stat['entries'] - 1

        for _ in range(msg_to_pop_num):
            self.queue.pop(sub_id=self.sub_id)
