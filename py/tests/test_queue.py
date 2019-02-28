#!/usr/bin/env python3
"""Test database."""

import time
import unittest
from typing import Set

import temppathlib

import persipubsub.control
import persipubsub.environment
import persipubsub.queue
import tests

# pylint: disable=missing-docstring
# pylint: disable=protected-access


def setup(env: persipubsub.environment.Environment,
          sub_set: Set[str]) -> persipubsub.control.Control:
    """Create an initialized control"""
    hwm = persipubsub.queue.HighWaterMark()
    strategy = persipubsub.queue.Strategy.PRUNE_FIRST

    control = env.new_control(
        subscriber_ids=sub_set, high_water_mark=hwm, strategy=strategy)

    return control


class TestQueue(unittest.TestCase):
    def test_initialize_environment(self) -> None:
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

    def test_put_to_single_subscriber(self) -> None:
        with temppathlib.TemporaryDirectory() as tmp_dir:
            msg = "I'm a message.".encode(tests.ENCODING)

            env = persipubsub.environment.Environment(path=tmp_dir.path)

            subscriber = 'sub'
            _ = setup(env=env, sub_set={subscriber})

            queue = env.new_publisher().queue
            assert queue is not None
            queue.put(msg=msg)

            assert queue.env is not None
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

    def test_put_multiple_subscriber(self) -> None:
        # pylint: disable=too-many-locals
        with temppathlib.TemporaryDirectory() as tmp_dir:
            msg = "I'm a message.".encode(tests.ENCODING)

            sub_set = {"sub", "another_sub"}

            env = persipubsub.environment.Environment(path=tmp_dir.path)
            _ = setup(env=env, sub_set=sub_set)

            queue = env.new_publisher().queue
            assert queue is not None
            queue.put(msg=msg)

            assert queue.env is not None
            with queue.env.begin() as txn:
                self.assertIsNotNone(
                    txn.get(key=list(sub_set)[0].encode(tests.ENCODING)))

                sub_db_0 = queue.env.open_db(
                    key=list(sub_set)[0].encode(tests.ENCODING),
                    txn=txn,
                    create=False)
                cursor = txn.cursor(db=sub_db_0)
                self.assertTrue(cursor.first())
                key_0 = cursor.key()

                self.assertIsNotNone(
                    txn.get(key=list(sub_set)[1].encode(tests.ENCODING)))

                sub_db_1 = queue.env.open_db(
                    key=list(sub_set)[1].encode(tests.ENCODING),
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

    def test_put_many(self) -> None:
        # pylint: disable=too-many-locals
        with temppathlib.TemporaryDirectory() as tmp_dir:
            env = persipubsub.environment.Environment(path=tmp_dir.path)

            subscriber = 'sub'
            _ = setup(env=env, sub_set={subscriber})

            queue = env.new_publisher().queue
            assert queue is not None

            msg = "I'm a message".encode(tests.ENCODING)
            msgs = []
            msg_num = 10
            for _ in range(msg_num):
                msgs.append(msg)

            queue.put_many_flush_once(msgs=msgs)

            assert queue.env is not None
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

    def test_front(self) -> None:
        with temppathlib.TemporaryDirectory() as tmp_dir:
            msg = "I'm a message.".encode(tests.ENCODING)

            env = persipubsub.environment.Environment(path=tmp_dir.path)

            subscriber = 'sub'
            _ = setup(env=env, sub_set={subscriber})

            queue = env.new_publisher().queue
            assert queue is not None
            queue.put(msg=msg)

            # pylint: disable=assignment-from-none
            # pylint: disable=assignment-from-no-return
            _, received_msg = queue.front(sub_id=subscriber)
            self.assertIsNotNone(received_msg)
            self.assertEqual(msg, received_msg)

    def test_pop(self) -> None:
        with temppathlib.TemporaryDirectory() as tmp_dir:
            msg = "I'm a message.".encode(tests.ENCODING)

            env = persipubsub.environment.Environment(path=tmp_dir.path)

            subscriber = 'sub'
            _ = setup(env=env, sub_set={subscriber})

            queue = env.new_publisher().queue
            assert queue is not None
            queue.put(msg=msg)

            # pylint: disable=assignment-from-none
            # pylint: disable=assignment-from-no-return
            _, received_msg = queue.front(sub_id=subscriber)
            self.assertIsNotNone(received_msg)

            assert queue.env is not None
            with queue.env.begin() as txn:
                pending_db = queue.env.open_db(
                    key=tests.PENDING_DB, txn=txn, create=False)

                cursor = txn.cursor(db=pending_db)
                self.assertTrue(cursor.first())
                pending_before_pop = cursor.value()

            queue.pop(sub_id=subscriber)

            _, received_msg = queue.front(sub_id=subscriber)

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

    def test_pop_queue_empty(self) -> None:
        with temppathlib.TemporaryDirectory() as tmp_dir:

            env = persipubsub.environment.Environment(path=tmp_dir.path)

            subscriber = 'sub'
            _ = setup(env=env, sub_set={subscriber})

            queue = env.new_publisher().queue
            assert queue is not None

            self.assertRaises(RuntimeError, queue.pop, sub_id=subscriber)

    def test_queue_initialisation(self) -> None:
        with temppathlib.TemporaryDirectory() as tmp_dir:
            env = persipubsub.environment.Environment(path=tmp_dir.path)

            subscriber = 'sub'
            control = setup(env=env, sub_set={subscriber})

            hwm = persipubsub.queue.HighWaterMark(
                message_timeout=tests.TEST_MSG_TIMEOUT,
                max_messages=tests.TEST_HWM_MSG_NUM,
                hwm_db_size=tests.TEST_HWM_LMDB_SIZE)

            control.set_hwm(hwm=hwm)
            control.set_strategy(
                strategy=persipubsub.queue.Strategy.PRUNE_FIRST)
            queue = env.new_control().queue
            assert queue is not None

            self.assertIsNotNone(queue.env)
            assert queue.env is not None
            self.assertEqual(tmp_dir.path.as_posix(), queue.env.path())
            assert queue.hwm is not None
            self.assertEqual(tests.TEST_HWM_LMDB_SIZE, queue.hwm.hwm_lmdb_size)
            self.assertEqual(tests.TEST_HWM_MSG_NUM, queue.hwm.max_messages)
            self.assertEqual(tests.TEST_MSG_TIMEOUT, queue.hwm.message_timeout)
            assert queue.strategy is not None
            self.assertEqual(persipubsub.queue.Strategy.PRUNE_FIRST.name,
                             queue.strategy.name)
            self.assertEqual({'sub'}, queue.subscriber_ids)

    def test_overflow_msgs_limit(self) -> None:
        with temppathlib.TemporaryDirectory() as tmp_dir:
            env = persipubsub.environment.Environment(path=tmp_dir.path)

            subscriber = 'sub'
            _ = setup(env=env, sub_set={subscriber})

            queue = env.new_publisher().queue
            assert queue is not None

            assert queue.hwm is not None
            queue.hwm.max_messages = tests.TEST_HWM_MSG_NUM

            msg = "hello world".encode(tests.ENCODING)

            self.assertEqual(0, queue.count_msgs())
            for _ in range(tests.TEST_HWM_MSG_NUM):
                queue.put(msg=msg)

            self.assertEqual(tests.TEST_HWM_MSG_NUM, queue.count_msgs())

            queue.put(msg=msg)

            self.assertEqual(
                int(tests.TEST_HWM_MSG_NUM - int(tests.TEST_HWM_MSG_NUM / 2)),
                queue.count_msgs())

    def test_overflow_limit_size(self) -> None:
        with temppathlib.TemporaryDirectory() as tmp_dir:
            env = persipubsub.environment.Environment(path=tmp_dir.path)

            subscriber = 'sub'
            _ = setup(env=env, sub_set={subscriber})

            queue = env.new_publisher().queue
            assert queue is not None

            assert queue.hwm is not None
            queue.hwm.hwm_lmdb_size = tests.TEST_HWM_LMDB_SIZE

            msg = ("a" * (int(tests.LMDB_PAGE_SIZE / 4))).encode(tests.ENCODING)

            while queue.check_current_lmdb_size() <= tests.TEST_HWM_LMDB_SIZE:
                queue.put(msg=msg)

            self.assertTrue(
                queue.check_current_lmdb_size() > tests.TEST_HWM_LMDB_SIZE)

            queue.put(msg=msg)

            self.assertTrue(
                queue.check_current_lmdb_size() <= tests.TEST_HWM_LMDB_SIZE)

    def test_timeout(self) -> None:
        with temppathlib.TemporaryDirectory() as tmp_dir:
            env = persipubsub.environment.Environment(path=tmp_dir.path)

            subscriber = 'sub'
            _ = setup(env=env, sub_set={subscriber})

            queue = env.new_publisher().queue
            assert queue is not None

            assert queue.hwm is not None
            queue.hwm.message_timeout = tests.TEST_MSG_TIMEOUT

            msg = "hello world".encode(tests.ENCODING)

            queue.put(msg=msg)
            self.assertEqual(1, queue.count_msgs())
            queue.put(msg=msg)
            self.assertEqual(2, queue.count_msgs())
            time.sleep(tests.TEST_MSG_TIMEOUT + 1)
            queue.put(msg=msg)
            self.assertEqual(1, queue.count_msgs())

    def test_strategy_prune_first(self) -> None:
        with temppathlib.TemporaryDirectory() as tmp_dir:
            env = persipubsub.environment.Environment(path=tmp_dir.path)

            subscriber = 'sub'
            _ = setup(env=env, sub_set={subscriber})

            queue = env.new_publisher().queue
            assert queue is not None
            queue.strategy = persipubsub.queue.Strategy.PRUNE_FIRST

            assert queue.hwm is not None
            queue.hwm.max_messages = tests.TEST_HWM_MSG_NUM

            for index in range(tests.TEST_HWM_MSG_NUM):
                msg = "secret message {}".format(index).encode(tests.ENCODING)
                queue.put(msg=msg)

            _, received_msg = queue.front(sub_id='sub')
            self.assertEqual("secret message 0".encode(tests.ENCODING),
                             received_msg)

            msg = "secret message {}".format(tests.TEST_HWM_MSG_NUM).encode(
                tests.ENCODING)
            queue.put(msg=msg)

            _, received_msg = queue.front(sub_id='sub')

            self.assertEqual(
                "secret message {}".format(
                    int((tests.TEST_HWM_MSG_NUM / 2) + 1)).encode(
                        tests.ENCODING), received_msg)

    def test_strategy_prune_last(self) -> None:
        with temppathlib.TemporaryDirectory() as tmp_dir:
            env = persipubsub.environment.Environment(path=tmp_dir.path)

            subscriber = 'sub'
            _ = setup(env=env, sub_set={subscriber})

            queue = env.new_publisher().queue
            assert queue is not None
            queue.strategy = persipubsub.queue.Strategy.PRUNE_LAST

            assert queue.hwm is not None
            queue.hwm.max_messages = tests.TEST_HWM_MSG_NUM

            for index in range(tests.TEST_HWM_MSG_NUM):
                msg = "secret message {}".format(index).encode(tests.ENCODING)
                queue.put(msg=msg)

            _, received_msg = queue.front(sub_id='sub')
            self.assertEqual("secret message 0".encode(tests.ENCODING),
                             received_msg)

            msg = "secret message {}".format(tests.TEST_HWM_MSG_NUM).encode(
                tests.ENCODING)
            queue.put(msg=msg)

            _, received_msg = queue.front(sub_id='sub')
            self.assertEqual("secret message 0".encode(tests.ENCODING),
                             received_msg)


if __name__ == '__main__':
    unittest.main()
