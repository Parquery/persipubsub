#!/usr/bin/env python
"""Receive messages persistent from publisher to subscriber."""

import contextlib
import pathlib
from typing import Union, Optional
import time


class Sub:
    """Create Subscriber ready to receive messages."""

    def __init__(self, sub_dir: Union[str, pathlib.Path], sub_id: str):
        """
        Initialize.

        :param sub_dir: Directory where subscriber data is stored
        :param sub_id: unique subscriber id
        """

    @contextlib.contextmanager
    def receive(self, timeout: int = 60) -> Optional[str]:
        """
        Receive messages from the publisher.

        :param timeout: time waiting for a message. If none arrived until the
            timeout then None will be returned.
        """
        # receive something

    def pop(self):
        """Pop a message from the subscriber's lmdb."""

    def pop_to_top(self):
        """
        Pops all messages until the most recent one.

        Used for slow processes to ensure realtime processing.
        """
