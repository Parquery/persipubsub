#!/usr/bin/env python
"""Distribute messages persistent from publisher to subscriber."""

import pathlib
from typing import Union


class Pub:
    """Create Publisher ready to send messages."""

    def __init__(self,
                 pub_dir: Union[str, pathlib.Path],
                 autosync: bool = False):
        """
        Initialize.

        :param pub_dir: Directory where publisher data is stored
        :param autosync: if True, store data automatically in lmdb
        """

    def send(self, msg: bytes):
        """
        Send messages to subscriber.

        :param msg: to send to all subscribers
        """

    def sync(self):
        """Store message on disk in lmdb."""
