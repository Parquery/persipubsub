#!/usr/bin/env python
"""Setup and control persistent distribution of messages."""

import pathlib
from typing import Union

import lmdb


class CTL:
    """Create control unit."""

    def __init__(self,
                 env_dir: Union[str, pathlib.Path],
                 max_readers: int=1024,
                 max_dbs: int = 1024,
                 max_db_size: int = 32 * 1024**3):
        """
        Initialize.

        :param env_dir: directory where lmdb is stored
        :param max_dbs: maximal number of databases
        :param max_db_size: maximal size of database in bytes
        """
        env_dir_str = env_dir if isinstance(env_dir, str) \
            else env_dir.as_posix()
        self.env = lmdb.open(path=env_dir_str, map_size=max_db_size,
                             subdir=True, max_readers=max_readers,
                             max_dbs=max_dbs)
        self.hwm = HighWaterMark()

    def clear_all_subs(self):
        """Clear all subscriber and delete all messages."""

    def add_sub(self, sub_id: str):
        """Add a subscriber and create its lmdb."""

    def remove_sub(self, sub_id: str):
        """Remove a subscriber and delete all its messages."""

    def prune_dangling_messages(self):
        """Prune all dangling messages from the lmdb."""

    def prune_all_messages_for(self, sub_id: str):
        """Prune all messages of a subscriber."""

    def set_hwm(self, msg_timeout: int = 500, max_msgs: int = 1024*64,
                hwm_lmdb_size: int = 30 * 1024**3):
        """
        Set high-water-mark limit.

        :param msg_timeout: time after which msg is classified as dangling msg
        :param max_msgs: maximal amount of msg
        :param hwm_lmdb_size: high water mark for total size of lmdb
        :return:
        """

    def set_overflow_strategy(self, strategy: bool = True):
        """
        Set the overflow strategy.

        :param strategy:
            | if True, the first messages will be pruned.
            | if False, the last messages will be pruned.
        :return:
        """


class HighWaterMark:

    def __init__(self, msg_timeout: int = 500, max_msgs: int = 1024*64,
                 hwm_lmdb_size: int = 30 * 1024**3):
        """
        Initialize.

        :param msg_timeout: time after which msg is classified as dangling msg
        :param max_msgs: maximal amount of msg
        :param hwm_lmdb_size: high water mark for total size of lmdb
        """
        self.msg_timeout = msg_timeout
        self.max_msgs = max_msgs
        self.hwm_lmdb_size = hwm_lmdb_size
