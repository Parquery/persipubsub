#!/usr/bin/env python
"""Distribute messages persistent from publisher to subscriber."""

import pathlib
from typing import List, Union

import persipubsub.queue


class Pub:
    """
    Create Publisher ready to send messages.

    :ivar pub_id: publisher ID
    :vartype pub_id: str
    :ivar queue: on which messages are published
    :vartype queue: persipubsub.queue.Queue
    :ivar sub_list: list of subscriber to which messages are send
    :vartype sub_list: List[str]
    :ivar autosync: sync after each message or after multiple messages
    :vartype autosync: bool
    """

    pub_id = None  # type: str
    queue = None  # type: persipubsub.queue.Queue
    sub_list = None  # type: List[str]
    autosync = None  # type: bool

    def __init__(self) -> None:
        """Initialize class object."""

    def init(self,
             pub_id: str,
             config_pth: Union[pathlib.Path, str],
             autosync: bool = False) -> None:
        """
        Initialize.

        :param pub_id: constant ID of publisher
        :param config_pth: path to the JSON config file
        :param autosync: if True, store data automatically in lmdb
        """
        self.pub_id = pub_id

        config = persipubsub.get_config(path=config_pth)
        publisher = config[pub_id]
        queue_dir = publisher["out_queue"]
        self.queue = persipubsub.queue.Queue()
        self.queue.init(config_pth=config_pth, queue_dir=queue_dir)
        assert isinstance(publisher["subscribers"], List)
        self.sub_list = publisher["subscribers"]  # type: List[str]
        self.autosync = autosync

    def __enter__(self) -> 'Pub':
        """Enter the context and give the pub prepared in the constructor."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Leave the context."""

    def send(self, msg: bytes) -> None:
        """
        Send message to subscribers.

        :param msg: to send to all subscribers
        """
        self.queue.put(msg=msg, sub_list=self.sub_list)

    def send_many(self, msgs: List[bytes]) -> None:
        """
        Send messages to subscribers.

        :param msgs: to send to all subscribers
        """
        if self.autosync:
            for msg in msgs:
                self.queue.put(msg=msg, sub_list=self.sub_list)
        else:
            self.queue.put_many_flush_once(msgs=msgs, sub_list=self.sub_list)
