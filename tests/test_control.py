#!/usr/bin/env python
"""Test control unit."""

import datetime
import pathlib
import unittest
from typing import List

import lmdb
import temppathlib

import persipubsub.control
import persipubsub.queue
import tests

# pylint: disable=missing-docstring
# pylint: disable=protected-access


def setup(path: pathlib.Path,
          sub_list: List[str]) -> persipubsub.control.Control:
    """Create an initialized control"""
    control = persipubsub.control.Control(path=path)

    hwm = persipubsub.queue.HighWaterMark()
    strategy = persipubsub.queue.Strategy.prune_first

    control.init(
        subscriber_ids=sub_list,
        max_readers=tests.TEST_MAX_READER_NUM,
        max_size=tests.TEST_MAX_DB_SIZE_BYTES,
        high_watermark=hwm,
        strategy=strategy)

    return control


class TestControl(unittest.TestCase):
    def test_initialize_all(self) -> None:
        with temppathlib.TemporaryDirectory() as tmp_dir:
            control = setup(path=tmp_dir.path, sub_list=['sub'])

            expected_db_keys = [
                b'data_db', b'meta_db', b'pending_db', b'queue_db', b'sub'
            ]
            db_keys = []  # type: List[bytes]

            assert isinstance(control.queue, persipubsub.queue._Queue)
            assert isinstance(control.queue.env, lmdb.Environment)
            with control.queue.env.begin() as txn:
                cursor = txn.cursor()
                cursor.first()
                for key, value in cursor:  # pylint: disable=unused-variable
                    db_keys.append(key)

            self.assertListEqual(sorted(expected_db_keys), sorted(db_keys))

    def test_del_sub(self) -> None:
        with temppathlib.TemporaryDirectory() as tmp_dir:
            control = setup(path=tmp_dir.path, sub_list=["sub1", "sub2"])

            assert isinstance(control.queue, persipubsub.queue._Queue)
            assert isinstance(control.queue.env, lmdb.Environment)

            control._remove_sub(sub_id="sub2", env=control.queue.env)

            expected_db_keys = [
                b'data_db', b'meta_db', b'pending_db', b'queue_db', b'sub1'
            ]
            db_keys = []  # type: List[bytes]

            with control.queue.env.begin() as txn:
                cursor = txn.cursor()
                cursor.first()
                for key, value in cursor:  # pylint: disable=unused-variable
                    db_keys.append(key)

            self.assertListEqual(sorted(expected_db_keys), sorted(db_keys))

    def test_clear_all_subs(self) -> None:
        with temppathlib.TemporaryDirectory() as tmp_dir:
            control = setup(path=tmp_dir.path, sub_list=["sub1", "sub2"])

            control.clear_all_subscribers()

            expected_db_keys = [
                b'data_db', b'meta_db', b'pending_db', b'queue_db'
            ]
            db_keys = []  # type: List[bytes]

            assert isinstance(control.queue, persipubsub.queue._Queue)
            assert isinstance(control.queue.env, lmdb.Environment)
            with control.queue.env.begin() as txn:
                cursor = txn.cursor()
                cursor.first()
                for key, value in cursor:  # pylint: disable=unused-variable
                    db_keys.append(key)

            self.assertListEqual(sorted(expected_db_keys), sorted(db_keys))

    def test_prune_dangling_messages(self) -> None:
        # pylint: disable=too-many-locals
        with temppathlib.TemporaryDirectory() as tmp_dir:
            control = setup(path=tmp_dir.path, sub_list=["sub"])

            assert isinstance(control.queue, persipubsub.queue._Queue)
            assert isinstance(control.queue.env, lmdb.Environment)
            assert isinstance(control.queue.hwm,
                              persipubsub.queue.HighWaterMark)
            control.queue.hwm.msg_timeout_secs = tests.TEST_MSG_TIMEOUT

            with control.queue.env.begin(write=True) as txn:
                sub_db = control.queue.env.open_db(
                    key='sub'.encode(tests.ENCODING), txn=txn, create=False)

                txn.put(key="timeout_msg".encode(tests.ENCODING), db=sub_db)
                txn.put(key="valid_msg".encode(tests.ENCODING), db=sub_db)
            with control.queue.env.begin(write=True) as txn:
                data_db = control.queue.env.open_db(
                    key=tests.DATA_DB, txn=txn, create=False)

                txn.put(
                    key="popped_msg".encode(tests.ENCODING),
                    value="I'm data".encode(tests.ENCODING),
                    db=data_db)
                txn.put(
                    key="timeout_msg".encode(tests.ENCODING),
                    value="I'm data too".encode(tests.ENCODING),
                    db=data_db)
                txn.put(
                    key="valid_msg".encode(tests.ENCODING),
                    value="Free me!".encode(tests.ENCODING),
                    db=data_db)
            with control.queue.env.begin(write=True) as txn:
                pending_db = control.queue.env.open_db(
                    key=tests.PENDING_DB, txn=txn, create=False)

                txn.put(
                    key="popped_msg".encode(tests.ENCODING),
                    value=int(0).to_bytes(
                        length=tests.BYTES_LENGTH, byteorder=tests.BYTES_ORDER),
                    db=pending_db)
                txn.put(
                    key="timeout_msg".encode(tests.ENCODING),
                    value=int(1).to_bytes(
                        length=tests.BYTES_LENGTH, byteorder=tests.BYTES_ORDER),
                    db=pending_db)
                txn.put(
                    key="valid_msg".encode(tests.ENCODING),
                    value=int(1).to_bytes(
                        length=tests.BYTES_LENGTH, byteorder=tests.BYTES_ORDER),
                    db=pending_db)

            with control.queue.env.begin(write=True) as txn:
                meta_db = control.queue.env.open_db(
                    key=tests.META_DB, txn=txn, create=False)

                txn.put(
                    key="popped_msg".encode(tests.ENCODING),
                    value=int(datetime.datetime.utcnow().timestamp()).to_bytes(
                        length=tests.BYTES_LENGTH, byteorder=tests.BYTES_ORDER),
                    db=meta_db)
                txn.put(
                    key="timeout_msg".encode(tests.ENCODING),
                    value=int(datetime.datetime.utcnow().timestamp() -
                              tests.TEST_MSG_TIMEOUT - 5).to_bytes(
                                  length=tests.BYTES_LENGTH,
                                  byteorder=tests.BYTES_ORDER),
                    db=meta_db)
                txn.put(
                    key="valid_msg".encode(tests.ENCODING),
                    value=int(datetime.datetime.utcnow().timestamp()).to_bytes(
                        length=tests.BYTES_LENGTH, byteorder=tests.BYTES_ORDER),
                    db=meta_db)

            control.prune_dangling_messages()

            dbs = [sub_db, data_db, pending_db, meta_db]

            expected_remaining_entries = 4
            remaining_entries = 0

            # pylint: disable=invalid-name
            for db in dbs:
                with control.queue.env.begin(db=db) as txn:
                    cursor = txn.cursor()
                    cursor.first()
                    for key, value in cursor:  # pylint: disable=unused-variable
                        remaining_entries += 1
                        self.assertEqual(b'valid_msg', key)

            self.assertEqual(expected_remaining_entries, remaining_entries)

    def test_prune_all_messages_for_subscriber(self) -> None:
        with temppathlib.TemporaryDirectory() as tmp_dir:
            control = setup(path=tmp_dir.path, sub_list=["sub"])

            msg = persipubsub.encoding("hello world!")
            assert isinstance(control.queue, persipubsub.queue._Queue)
            assert isinstance(control.queue.env, lmdb.Environment)
            control.queue.put(msg=msg)
            control.queue.put(msg=msg)

            with control.queue.env.begin(write=False) as txn:
                sub_db = control.queue.env.open_db(
                    key=persipubsub.encoding('sub'), txn=txn, create=False)
                sub_stat = txn.stat(db=sub_db)
                self.assertEqual(2, sub_stat['entries'])

            control._prune_all_messages_for(sub_id="sub")

            with control.queue.env.begin(write=False) as txn:
                sub_db = control.queue.env.open_db(
                    key=persipubsub.encoding('sub'), txn=txn, create=False)
                sub_stat = txn.stat(db=sub_db)
                self.assertEqual(0, sub_stat['entries'])

    def test_is_initialized(self) -> None:
        with temppathlib.TemporaryDirectory() as tmp_dir:
            control = setup(path=tmp_dir.path, sub_list=["sub"])

            self.assertTrue(control.check_queue_is_initialized())

    def test_is_not_initialized(self) -> None:
        with temppathlib.TemporaryDirectory() as tmp_dir:
            control = persipubsub.control.Control(path=tmp_dir.path)
            self.assertFalse(control.check_queue_is_initialized())


if __name__ == '__main__':
    unittest.main()
