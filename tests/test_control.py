#!/usr/bin/env python
"""Test control unit."""

import datetime
import json
import unittest
from typing import List

import temppathlib

import persipubsub.control
import persipubsub.queue
import tests

# pylint: disable=missing-docstring


class TestControl(unittest.TestCase):
    def test_initialize_all(self):
        with temppathlib.TemporaryDirectory() as tmp_dir:
            config = tests.generate_test_config(path=tmp_dir.path)

            file = tmp_dir.path / "config.json"

            with open(file=file.as_posix(), mode='wt') as file_object:
                json.dump(config, file_object)

            persipubsub.control.initialize_all_dbs(config_pth=file)

            queue = persipubsub.queue.Queue()
            queue.init(config_pth=file, queue_dir=tmp_dir.path / "queue")

            expected_db_keys = [b'data_db', b'meta_db', b'pending_db', b'sub']
            db_keys = []  # type: List[bytes]

            with queue.env.begin() as txn:
                cursor = txn.cursor()
                cursor.first()
                for key, value in cursor:  # pylint: disable=unused-variable
                    db_keys.append(key)

            self.assertListEqual(sorted(expected_db_keys), sorted(db_keys))

    def test_add_sub(self):
        with temppathlib.TemporaryDirectory() as tmp_dir:
            config = tests.generate_test_config(path=tmp_dir.path)

            file = tmp_dir.path / "config.json"

            with open(file=file.as_posix(), mode='wt') as file_object:
                json.dump(config, file_object)

            queue = persipubsub.queue.Queue()
            queue.init(config_pth=file, queue_dir=tmp_dir.path / "queue")

            persipubsub.control.add_sub(sub_id="sub1", queue=queue)
            persipubsub.control.add_sub(sub_id="sub2", queue=queue)

            expected_db_keys = [
                b'data_db', b'meta_db', b'pending_db', b'sub1', b'sub2'
            ]
            db_keys = []  # type: List[bytes]

            with queue.env.begin() as txn:
                cursor = txn.cursor()
                cursor.first()
                for key, value in cursor:  # pylint: disable=unused-variable
                    db_keys.append(key)

            self.assertListEqual(sorted(expected_db_keys), sorted(db_keys))

    def test_del_sub(self):
        with temppathlib.TemporaryDirectory() as tmp_dir:
            config = tests.generate_test_config(path=tmp_dir.path)

            file = tmp_dir.path / "config.json"

            with open(file=file.as_posix(), mode='wt') as file_object:
                json.dump(config, file_object)

            queue = persipubsub.queue.Queue()
            queue.init(config_pth=file, queue_dir=tmp_dir.path / "queue")

            persipubsub.control.add_sub(sub_id="sub1", queue=queue)
            persipubsub.control.add_sub(sub_id="sub2", queue=queue)
            persipubsub.control.remove_sub(sub_id="sub2", queue=queue)

            expected_db_keys = [b'data_db', b'meta_db', b'pending_db', b'sub1']
            db_keys = []  # type: List[bytes]

            with queue.env.begin() as txn:
                cursor = txn.cursor()
                cursor.first()
                for key, value in cursor:  # pylint: disable=unused-variable
                    db_keys.append(key)

            self.assertListEqual(sorted(expected_db_keys), sorted(db_keys))

    def test_clear_all_subs(self):
        with temppathlib.TemporaryDirectory() as tmp_dir:
            config = tests.generate_test_config(path=tmp_dir.path)

            file = tmp_dir.path / "config.json"

            with open(file=file.as_posix(), mode='wt') as file_object:
                json.dump(config, file_object)

            queue = persipubsub.queue.Queue()
            queue.init(config_pth=file, queue_dir=tmp_dir.path / "queue")

            persipubsub.control.initialize_all_dbs(config_pth=file)
            persipubsub.control.clear_all_subs(config_pth=file)

            expected_db_keys = [b'data_db', b'meta_db', b'pending_db']
            db_keys = []  # type: List[bytes]

            with queue.env.begin() as txn:
                cursor = txn.cursor()
                cursor.first()
                for key, value in cursor:  # pylint: disable=unused-variable
                    db_keys.append(key)

            self.assertListEqual(sorted(expected_db_keys), sorted(db_keys))

    def test_prune_dangling_messages(self):
        # pylint: disable=too-many-locals
        with temppathlib.TemporaryDirectory() as tmp_dir:
            config = tests.generate_test_config(path=tmp_dir.path)

            file = tmp_dir.path / "config.json"

            with open(file=file.as_posix(), mode='wt') as file_object:
                json.dump(config, file_object)

            persipubsub.control.initialize_all_dbs(config_pth=file)

            queue = persipubsub.queue.Queue()
            queue.init(config_pth=file, queue_dir=tmp_dir.path / "queue")
            queue.hwm.msg_timeout_secs = tests.TEST_MSG_TIMEOUT

            with queue.env.begin(write=True) as txn:
                sub_db = queue.env.open_db(
                    key='sub'.encode(tests.ENCODING), txn=txn, create=False)

                txn.put(key="timeout_msg".encode(tests.ENCODING), db=sub_db)
                txn.put(key="valid_msg".encode(tests.ENCODING), db=sub_db)
            with queue.env.begin(write=True) as txn:
                data_db = queue.env.open_db(
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
            with queue.env.begin(write=True) as txn:
                pending_db = queue.env.open_db(
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

            with queue.env.begin(write=True) as txn:
                meta_db = queue.env.open_db(
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

            persipubsub.control.prune_dangling_messages(config_pth=file)

            dbs = [sub_db, data_db, pending_db, meta_db]

            expected_remaining_entries = 4
            remaining_entries = 0

            # pylint: disable=invalid-name
            for db in dbs:
                with queue.env.begin(db=db) as txn:
                    cursor = txn.cursor()
                    cursor.first()
                    for key, value in cursor:  # pylint: disable=unused-variable
                        remaining_entries += 1
                        self.assertEqual(b'valid_msg', key)

            self.assertEqual(expected_remaining_entries, remaining_entries)

    def test_prune_all_messages_for_subscriber(self):
        with temppathlib.TemporaryDirectory() as tmp_dir:
            config = tests.generate_test_config(path=tmp_dir.path)

            file = tmp_dir.path / "config.json"

            with open(file=file.as_posix(), mode='wt') as file_object:
                json.dump(config, file_object)

            queue = persipubsub.queue.Queue()
            queue.init(config_pth=file, queue_dir=tmp_dir.path / "queue")

            persipubsub.control.initialize_all_dbs(config_pth=file)

            msg = persipubsub.encoding("hello world!")

            queue.put(msg=msg, sub_list=["sub"])
            queue.put(msg=msg, sub_list=["sub"])

            with queue.env.begin(write=False) as txn:
                sub_db = queue.env.open_db(
                    key=persipubsub.encoding('sub'), txn=txn, create=False)
                sub_stat = txn.stat(db=sub_db)
                self.assertEqual(2, sub_stat['entries'])

            persipubsub.control.prune_all_messages_for(
                sub_id="sub", queue=queue)

            with queue.env.begin(write=False) as txn:
                sub_db = queue.env.open_db(
                    key=persipubsub.encoding('sub'), txn=txn, create=False)
                sub_stat = txn.stat(db=sub_db)
                self.assertEqual(0, sub_stat['entries'])


if __name__ == '__main__':
    unittest.main()
