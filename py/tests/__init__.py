#!/usr/bin/env python3
"""Test persipubsub."""

import unittest

import lmdb
import temppathlib

import persipubsub
# pylint: disable=missing-docstring
import persipubsub.database

LMDB_PAGE_SIZE = 4096

TEST_MSG_TIMEOUT = 1  # type: int
TEST_HWM_MSG_NUM = 10  # type: int
TEST_HWM_LMDB_SIZE = LMDB_PAGE_SIZE * 2  # type: int

TEST_MAX_DB_NUM = 1024  # type: int
TEST_MAX_DB_SIZE = TEST_HWM_LMDB_SIZE * TEST_MAX_DB_NUM  # type: int
TEST_MAX_READERS = 1024  # type: int

# define all encodings here
ENCODING = 'utf-8'
BYTES_LENGTH = 8
BYTES_ORDER = 'big'

# define all database names here
DATA_DB = "data_db".encode(ENCODING)  # msg_id | data
PENDING_DB = "pending_db".encode(ENCODING)  # msg_id | pending subscriber
META_DB = "meta_db".encode(ENCODING)  # msg_id | metadata
QUEUE_DB = "queue_db".encode(ENCODING)  # queue_pth | all queue data
SUBSCRIBER_DB = "subscriber_db".encode(ENCODING)  # sub_id | -


class TestPersiPubSub(unittest.TestCase):
    def test_get_data(self) -> None:
        with temppathlib.TemporaryDirectory() as tmp_dir:
            env = lmdb.open(path=tmp_dir.path.as_posix(), max_dbs=2)

            with env.begin(write=True) as txn:
                queue_db = env.open_db(key=QUEUE_DB, txn=txn)
                txn.put(
                    key=persipubsub.database.MESSAGE_TIMEOUT_KEY,
                    value=persipubsub.database.int_to_bytes(500),
                    db=queue_db)
                txn.put(
                    key=persipubsub.database.MAX_MESSAGES_KEY,
                    value=persipubsub.database.int_to_bytes(1000),
                    db=queue_db)
                txn.put(
                    key=persipubsub.database.HWM_DB_SIZE_KEY,
                    value=persipubsub.database.int_to_bytes(1024**3),
                    db=queue_db)
                txn.put(
                    key=persipubsub.database.STRATEGY_KEY,
                    value=persipubsub.database.str_to_bytes("prune_last"),
                    db=queue_db)

                subscriber_db = env.open_db(key=SUBSCRIBER_DB, txn=txn)
                txn.put(
                    key=persipubsub.database.str_to_bytes(""), db=subscriber_db)

            queue_data = persipubsub.database.retrieve_queue_data(env=env)
            env.close()

        self.assertEqual(500, queue_data.message_timeout)
        self.assertEqual(1000, queue_data.max_messages)
        self.assertEqual(1024**3, queue_data.hwm_db_size)
        self.assertEqual("prune_last", queue_data.strategy)
        self.assertEqual([], queue_data.subscriber_ids)


if __name__ == '__main__':
    unittest.main()
