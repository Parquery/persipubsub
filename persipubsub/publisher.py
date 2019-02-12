#!/usr/bin/env python3
"""Publish messages to a queue and save them persistently."""

import pathlib
from typing import Any, List, Optional, Union

import lmdb

import persipubsub.queue

# pylint: disable=protected-access


class Publisher:
    """
    Handle publishing messages to the queue.

    :ivar queue: on which messages are published
    :vartype queue: persipubsub.queue.Queue
    :ivar autosync: sync after each message or after multiple messages
    :vartype autosync: bool
    """

    def __init__(self) -> None:
        """Initialize class object."""
        self.queue = None  # type: Optional[persipubsub.queue._Queue]
        self.autosync = None  # type: Optional[bool]

    def init(self,
             path: Union[pathlib.Path, str],
             env: Optional[lmdb.Environment] = None,
             autosync: bool = False) -> None:
        """
        Initialize.

        :param path: path to the queue
        :param env: open lmdb environment
        :param autosync: if True, store data automatically in lmdb
        """
        self.queue = persipubsub.queue._Queue()  # pylint: disable=protected-access
        self.queue.init(path=path, env=env)
        self.autosync = autosync

    def __enter__(self) -> 'Publisher':
        """Enter the context and give the pub prepared in the constructor."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Leave the context."""

    def send(self, msg: bytes) -> None:
        """
        Write one message to queue in one transaction.

        :param msg: to queue that all subscribers can read it
        """
        assert isinstance(self.queue, persipubsub.queue._Queue)
        self.queue.put(msg=msg)

    def send_many(self, msgs: List[bytes]) -> None:
        """
        Write multiple messages to queue in one transaction.

        :param msgs: to queue that all subscribers can read them
        """
        assert isinstance(self.queue, persipubsub.queue._Queue)
        if self.autosync:
            for msg in msgs:
                self.queue.put(msg=msg)
        else:
            self.queue.put_many_flush_once(msgs=msgs)
