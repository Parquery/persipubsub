#!/usr/bin/env python3
"""Test database."""

import pathlib
import time
import unittest
from typing import List

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

    hwm = persipubsub.queue.HighWaterMark(
        msg_timeout_secs=tests.TEST_MSG_TIMEOUT,
        max_msgs_num=tests.TEST_HWM_MSG_NUM,
        hwm_lmdb_size_bytes=tests.TEST_HWM_LMDB_SIZE)
    strategy = persipubsub.queue.Strategy.prune_first

    control.init(
        subscriber_ids=sub_list,
        max_readers=tests.TEST_MAX_READER_NUM,
        max_size=tests.TEST_MAX_DB_SIZE_BYTES,
        high_watermark=hwm,
        strategy=strategy)

    return control


class TestQueue(unittest.TestCase):
    def test_initialize_environment(self):
        with temppathlib.TemporaryDirectory() as tmp_dir:
            env = persipubsub.queue._initialize_environment(
                queue_dir=tmp_dir.path)

            self.assertDictEqual({
                'branch_pages': 0,
                'entries': 0,
                'depth': 0,
                'leaf_pages': 0,
                'overflow_pages': 0,
                'psize': 4096
            }, env.stat())
            self.assertDictEqual({
                'last_txnid': 0,
                'last_pgno': 1,
                'map_size': 32 * 1024**3,
                'map_addr': 0,
                'max_readers': 1024,
                'num_readers': 1
            }, env.info())

    def test_put_to_single_subscriber(self):
        with temppathlib.TemporaryDirectory() as tmp_dir:
            msg = "I'm a message.".encode(tests.ENCODING)

            subscriber = "sub"

            _ = setup(path=tmp_dir.path, sub_list=['sub'])

            queue = persipubsub.queue._Queue()
            queue.init(path=tmp_dir.path)
            queue.put(msg=msg)

            with queue.env.begin() as txn:
                self.assertIsNotNone(
                    txn.get(key=subscriber.encode(tests.ENCODING)))

                sub_db = queue.env.open_db(
                    key=subscriber.encode(tests.ENCODING),
                    txn=txn,
                    create=False)
                cursor = txn.cursor(db=sub_db)
                self.assertTrue(cursor.first())
                key = cursor.key()

                data_db = queue.env.open_db(
                    key=tests.DATA_DB, txn=txn, create=False)

                value = txn.get(key=key, db=data_db)
                self.assertIsNotNone(value)
                self.assertEqual(msg, value)

    def test_put_multiple_subscriber(self):
        # pylint: disable=too-many-locals
        with temppathlib.TemporaryDirectory() as tmp_dir:
            msg = "I'm a message.".encode(tests.ENCODING)

            sub_list = ["sub", "another_sub"]

            _ = setup(path=tmp_dir.path, sub_list=sub_list)

            queue = persipubsub.queue._Queue()
            queue.init(path=tmp_dir.path)

            queue.put(msg=msg)

            with queue.env.begin() as txn:
                self.assertIsNotNone(
                    txn.get(key=sub_list[0].encode(tests.ENCODING)))

                sub_db_0 = queue.env.open_db(
                    key=sub_list[0].encode(tests.ENCODING),
                    txn=txn,
                    create=False)
                cursor = txn.cursor(db=sub_db_0)
                self.assertTrue(cursor.first())
                key_0 = cursor.key()

                self.assertIsNotNone(
                    txn.get(key=sub_list[1].encode(tests.ENCODING)))

                sub_db_1 = queue.env.open_db(
                    key=sub_list[1].encode(tests.ENCODING),
                    txn=txn,
                    create=False)
                cursor = txn.cursor(db=sub_db_1)
                self.assertTrue(cursor.first())
                key_1 = cursor.key()

                self.assertEqual(key_0, key_1)

                data_db = queue.env.open_db(
                    key=tests.DATA_DB, txn=txn, create=False)

                value = txn.get(key=key_1, db=data_db)
                self.assertIsNotNone(value)
                self.assertEqual(msg, value)

    def test_put_many(self):
        # pylint: disable=too-many-locals
        with temppathlib.TemporaryDirectory() as tmp_dir:
            subscriber = "sub"
            _ = setup(path=tmp_dir.path, sub_list=[subscriber])

            queue = persipubsub.queue._Queue()
            queue.init(path=tmp_dir.path)

            msg = "I'm a message".encode(tests.ENCODING)
            msgs = []
            msg_num = 10
            for _ in range(msg_num):
                msgs.append(msg)

            queue.put_many_flush_once(msgs=msgs)

            with queue.env.begin(write=False) as txn:
                self.assertIsNotNone(
                    txn.get(key=subscriber.encode(tests.ENCODING)))

                sub_db = queue.env.open_db(
                    key=subscriber.encode(tests.ENCODING),
                    txn=txn,
                    create=False)

                sub_stat = txn.stat(db=sub_db)
                self.assertEqual(msg_num, sub_stat['entries'])

                data_db = queue.env.open_db(
                    key=tests.DATA_DB, txn=txn, create=False)

                data_stat = txn.stat(db=data_db)
                self.assertEqual(msg_num, data_stat['entries'])

    def test_front(self):
        with temppathlib.TemporaryDirectory() as tmp_dir:
            msg = "I'm a message.".encode(tests.ENCODING)

            subscriber = "sub"
            _ = setup(path=tmp_dir.path, sub_list=[subscriber])

            queue = persipubsub.queue._Queue()
            queue.init(path=tmp_dir.path)
            queue.put(msg=msg)

            # pylint: disable=assignment-from-none
            # pylint: disable=assignment-from-no-return
            received_msg = queue.front(identifier=subscriber)
            self.assertIsNotNone(received_msg)
            self.assertEqual(msg, received_msg)

    def test_pop(self):
        with temppathlib.TemporaryDirectory() as tmp_dir:
            msg = "I'm a message.".encode(tests.ENCODING)

            subscriber = "sub"
            _ = setup(path=tmp_dir.path, sub_list=[subscriber])

            queue = persipubsub.queue._Queue()
            queue.init(path=tmp_dir.path)
            queue.put(msg=msg)

            # pylint: disable=assignment-from-none
            # pylint: disable=assignment-from-no-return
            received_msg = queue.front(identifier=subscriber)
            self.assertIsNotNone(received_msg)

            with queue.env.begin() as txn:
                pending_db = queue.env.open_db(
                    key=tests.PENDING_DB, txn=txn, create=False)

                cursor = txn.cursor(db=pending_db)
                self.assertTrue(cursor.first())
                pending_before_pop = cursor.value()

            queue.pop(identifier=subscriber)

            received_msg = queue.front(identifier=subscriber)
            self.assertIsNone(received_msg)

            with queue.env.begin() as txn:
                pending_db = queue.env.open_db(
                    key=tests.PENDING_DB, txn=txn, create=False)
                cursor = txn.cursor(db=pending_db)
                self.assertTrue(cursor.first())
                pending_after_pop = cursor.value()

            self.assertEqual(
                int.from_bytes(pending_before_pop, tests.BYTES_ORDER) - 1,
                int.from_bytes(pending_after_pop, tests.BYTES_ORDER))

    def test_pop_queue_empty(self):
        with temppathlib.TemporaryDirectory() as tmp_dir:

            subscriber = "sub"
            _ = setup(path=tmp_dir.path, sub_list=[subscriber])

            queue = persipubsub.queue._Queue()
            queue.init(path=tmp_dir.path)

            self.assertRaises(RuntimeError, queue.pop, identifier=subscriber)

    def test_queue_initialisation(self):
        with temppathlib.TemporaryDirectory() as tmp_dir:
            _ = setup(path=tmp_dir.path, sub_list=['sub'])

            queue = persipubsub.queue._Queue()
            queue.init(path=tmp_dir.path)

            self.assertIsNotNone(queue.env)
            self.assertEqual(tmp_dir.path.as_posix(), queue.env.path())
            self.assertEqual(tests.TEST_HWM_LMDB_SIZE,
                             queue.hwm.hwm_lmdb_size_bytes)
            self.assertEqual(tests.TEST_HWM_MSG_NUM, queue.hwm.max_msgs_num)
            self.assertEqual(tests.TEST_MSG_TIMEOUT, queue.hwm.msg_timeout_secs)
            self.assertEqual(persipubsub.queue.Strategy.prune_first.name,
                             queue.strategy.name)
            self.assertEqual(['sub'], queue.subscriber_ids)

    def test_overflow_msgs_limit(self):
        with temppathlib.TemporaryDirectory() as tmp_dir:
            _ = setup(path=tmp_dir.path, sub_list=['sub'])

            queue = persipubsub.queue._Queue()
            queue.init(path=tmp_dir.path)

            queue.hwm.max_msgs_num = tests.TEST_HWM_MSG_NUM

            msg = "hello world".encode(tests.ENCODING)

            self.assertEqual(0, queue.count_msgs())
            for _ in range(tests.TEST_HWM_MSG_NUM):
                queue.put(msg=msg)

            self.assertEqual(tests.TEST_HWM_MSG_NUM, queue.count_msgs())

            queue.put(msg=msg)

            self.assertEqual(
                int(tests.TEST_HWM_MSG_NUM - int(tests.TEST_HWM_MSG_NUM / 2)),
                queue.count_msgs())

    def test_overflow_limit_size(self):
        with temppathlib.TemporaryDirectory() as tmp_dir:
            _ = setup(path=tmp_dir.path, sub_list=['sub'])

            queue = persipubsub.queue._Queue()
            queue.init(path=tmp_dir.path)

            queue.hwm.hwm_lmdb_size_bytes = tests.TEST_HWM_LMDB_SIZE

            msg = ("a" * (int(tests.LMDB_PAGE_SIZE / 4))).encode(tests.ENCODING)

            while queue.check_current_lmdb_size() <= tests.TEST_HWM_LMDB_SIZE:
                queue.put(msg=msg)

            self.assertTrue(
                queue.check_current_lmdb_size() > tests.TEST_HWM_LMDB_SIZE)

            queue.put(msg=msg)

            self.assertTrue(
                queue.check_current_lmdb_size() <= tests.TEST_HWM_LMDB_SIZE)

    def test_timeout(self):
        with temppathlib.TemporaryDirectory() as tmp_dir:
            _ = setup(path=tmp_dir.path, sub_list=['sub'])

            queue = persipubsub.queue._Queue()
            queue.init(path=tmp_dir.path)
            queue.hwm.msg_timeout_secs = tests.TEST_MSG_TIMEOUT

            msg = "hello world".encode(tests.ENCODING)

            queue.put(msg=msg)
            self.assertEqual(1, queue.count_msgs())
            queue.put(msg=msg)
            self.assertEqual(2, queue.count_msgs())
            time.sleep(tests.TEST_MSG_TIMEOUT + 1)
            queue.put(msg=msg)
            self.assertEqual(1, queue.count_msgs())

    def test_strategy_prune_first(self):
        with temppathlib.TemporaryDirectory() as tmp_dir:
            _ = setup(path=tmp_dir.path, sub_list=['sub'])

            queue = persipubsub.queue._Queue()
            queue.init(path=tmp_dir.path)
            queue.strategy = persipubsub.queue.Strategy.prune_first

            queue.hwm.max_msgs_num = tests.TEST_HWM_MSG_NUM

            for index in range(tests.TEST_HWM_MSG_NUM):
                msg = "secret message {}".format(index).encode(tests.ENCODING)
                queue.put(msg=msg)

            self.assertEqual("secret message 0".encode(tests.ENCODING),
                             queue.front(identifier='sub'))

            msg = "secret message {}".format(tests.TEST_HWM_MSG_NUM).encode(
                tests.ENCODING)
            queue.put(msg=msg)

            self.assertEqual(
                "secret message {}".format(
                    int((tests.TEST_HWM_MSG_NUM / 2) + 1)).encode(
                        tests.ENCODING), queue.front(identifier='sub'))

    def test_strategy_prune_last(self):
        with temppathlib.TemporaryDirectory() as tmp_dir:
            _ = setup(path=tmp_dir.path, sub_list=['sub'])

            queue = persipubsub.queue._Queue()
            queue.init(path=tmp_dir.path)
            queue.strategy = persipubsub.queue.Strategy.prune_last

            queue.hwm.max_msgs_num = tests.TEST_HWM_MSG_NUM

            for index in range(tests.TEST_HWM_MSG_NUM):
                msg = "secret message {}".format(index).encode(tests.ENCODING)
                queue.put(msg=msg)

            self.assertEqual("secret message 0".encode(tests.ENCODING),
                             queue.front(identifier='sub'))

            msg = "secret message {}".format(tests.TEST_HWM_MSG_NUM).encode(
                tests.ENCODING)
            queue.put(msg=msg)

            self.assertEqual("secret message 0".encode(tests.ENCODING),
                             queue.front(identifier='sub'))


if __name__ == '__main__':
    unittest.main()
