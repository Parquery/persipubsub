#!/usr/bin/env python3
"""Test persipubsub."""

import json
import pathlib
import unittest
from typing import Any, Dict

import lmdb
import temppathlib

import persipubsub

# pylint: disable=missing-docstring

LMDB_PAGE_SIZE = 4096

TEST_MSG_TIMEOUT = 1  # type: int
TEST_HWM_MSG_NUM = 10  # type: int
TEST_HWM_LMDB_SIZE = LMDB_PAGE_SIZE * 2  # type: int

TEST_MAX_DB_NUM = 1024  # type: int
TEST_MAX_DB_SIZE_BYTES = TEST_HWM_LMDB_SIZE * TEST_MAX_DB_NUM  # type: int
TEST_MAX_READER_NUM = 1024  # type: int

# define all encodings here
ENCODING = 'utf-8'
BYTES_LENGTH = 8
BYTES_ORDER = 'big'

# define all database names here
DATA_DB = "data_db".encode(ENCODING)  # msg_id | data
PENDING_DB = "pending_db".encode(ENCODING)  # msg_id | pending subscriber
META_DB = "meta_db".encode(ENCODING)  # msg_id | metadata
QUEUE_DB = "queue_db".encode(ENCODING)  # queue_pth | all queue data


class TestPersiPubSub(unittest.TestCase):
    def test_get_data(self) -> None:
        with temppathlib.TemporaryDirectory() as tmp_dir:
            env = lmdb.open(path=tmp_dir.path.as_posix(), max_dbs=2)

            with env.begin(write=True) as txn:
                queue_db = env.open_db(key=QUEUE_DB, txn=txn)
                txn.put(
                    key="key".encode(ENCODING),
                    value="value".encode(ENCODING),
                    db=queue_db)

            data = persipubsub.get_queue_data(
                key="key".encode(ENCODING), env=env)
        self.assertEqual(b'value', data)


if __name__ == '__main__':
    unittest.main()
