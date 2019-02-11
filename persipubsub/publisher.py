#!/usr/bin/env python3
"""Distribute messages persistent from publisher to subscriber."""

import pathlib
from typing import Any, List, Optional, Union

import persipubsub.queue

# pylint: disable=protected-access


class Publisher:
    """
    Create Publisher ready to send messages.

    :ivar queue: on which messages are published
    :vartype queue: persipubsub.queue.Queue
    :ivar autosync: sync after each message or after multiple messages
    :vartype autosync: bool
    """

    def __init__(self) -> None:
        """Initialize class object."""
        self.queue = None  # type: Optional[persipubsub.queue._Queue]
        self.autosync = None  # type: Optional[bool]

    def init(self, path: Union[pathlib.Path, str],
             autosync: bool = False) -> None:
        """
        Initialize.

        :param path: path to the queue
        :param autosync: if True, store data automatically in lmdb
        """
        self.queue = persipubsub.queue._Queue()  # pylint: disable=protected-access
        self.queue.init(path=path)
        self.autosync = autosync

    def __enter__(self) -> 'Publisher':
        """Enter the context and give the pub prepared in the constructor."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Leave the context."""

    def send(self, msg: bytes) -> None:
        """
        Send message to subscribers.

        :param msg: to send to all subscribers
        """
        assert isinstance(self.queue, persipubsub.queue._Queue)
        self.queue.put(msg=msg)

    def send_many(self, msgs: List[bytes]) -> None:
        """
        Send messages to subscribers.

        :param msgs: to send to all subscribers
        """
        assert isinstance(self.queue, persipubsub.queue._Queue)
        if self.autosync:
            for msg in msgs:
                self.queue.put(msg=msg)
        else:
            self.queue.put_many_flush_once(msgs=msgs)
