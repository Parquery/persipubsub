#!/usr/bin/env python3
"""Test database."""

import json
import time
import unittest

import temppathlib

import persipubsub.control
import persipubsub.queue
import tests

# pylint: disable=missing-docstring


class TestQueue(unittest.TestCase):
    def test_initialize_environment(self):
        with temppathlib.TemporaryDirectory() as tmp_dir:
            env = persipubsub.queue.initialize_environment(
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
            msg = "I'm a protobuf message.".encode(tests.ENCODING)

            subscriber = "sub"

            config = tests.generate_test_config(path=tmp_dir.path)

            file = tmp_dir.path / "config.json"

            with open(file=file.as_posix(), mode='wt') as file_object:
                json.dump(config, file_object)

            persipubsub.control.initialize_all_dbs(config_pth=file)
            queue = persipubsub.queue.Queue()
            queue.init(config_pth=file, queue_dir=tmp_dir.path / "queue")
            queue.put(msg=msg, sub_list=[subscriber])

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
        # TODO(snaji):
        pass

    def test_put_many(self):
        # pylint: disable=too-many-locals
        with temppathlib.TemporaryDirectory() as tmp_dir:
            config = tests.generate_test_config(path=tmp_dir.path)

            file = tmp_dir.path / "config.json"

            with open(file=file.as_posix(), mode='wt') as file_object:
                json.dump(config, file_object)

            persipubsub.control.initialize_all_dbs(config_pth=file)
            queue = persipubsub.queue.Queue()
            queue.init(config_pth=file, queue_dir=tmp_dir.path / "queue")

            # TODO(snaji): check everywhere encoding
            msg = "I'm a message".encode(tests.ENCODING)
            msgs = []
            msg_num = 10
            for _ in range(msg_num):
                msgs.append(msg)

            subscriber = "sub"

            queue.put_many_flush_once(msgs=msgs, sub_list=[subscriber])

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
            msg = "I'm a protobuf message.".encode(tests.ENCODING)

            subscriber = "sub"

            config = tests.generate_test_config(path=tmp_dir.path)

            file = tmp_dir.path / "config.json"

            with open(file=file.as_posix(), mode='wt') as file_object:
                json.dump(config, file_object)

            persipubsub.control.initialize_all_dbs(config_pth=file)

            queue = persipubsub.queue.Queue()
            queue.init(config_pth=file, queue_dir=tmp_dir.path / "queue")
            # TODO(snaji): replace with lmdb.env statements?
            queue.put(msg=msg, sub_list=[subscriber])

            # pylint: disable=assignment-from-none
            # pylint: disable=assignment-from-no-return
            received_msg = queue.front(sub_id=subscriber)
            self.assertIsNotNone(received_msg)
            self.assertEqual(msg, received_msg)

    def test_pop(self):
        with temppathlib.TemporaryDirectory() as tmp_dir:
            msg = "I'm a protobuf message.".encode(tests.ENCODING)

            subscriber = "sub"

            config = tests.generate_test_config(path=tmp_dir.path)

            file = tmp_dir.path / "config.json"

            with open(file=file.as_posix(), mode='wt') as file_object:
                json.dump(config, file_object)

            persipubsub.control.initialize_all_dbs(config_pth=file)

            queue = persipubsub.queue.Queue()
            queue.init(config_pth=file, queue_dir=tmp_dir.path / "queue")
            # TODO(snaji): replace with lmdb.env statements?
            queue.put(msg=msg, sub_list=[subscriber])

            # pylint: disable=assignment-from-none
            # pylint: disable=assignment-from-no-return
            received_msg = queue.front(sub_id=subscriber)
            self.assertIsNotNone(received_msg)

            with queue.env.begin() as txn:
                pending_db = queue.env.open_db(
                    key=tests.PENDING_DB, txn=txn, create=False)

                cursor = txn.cursor(db=pending_db)
                self.assertTrue(cursor.first())
                pending_before_pop = cursor.value()

            queue.pop(sub_id=subscriber)

            received_msg = queue.front(sub_id=subscriber)
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

            config = tests.generate_test_config(path=tmp_dir.path)

            file = tmp_dir.path / "config.json"

            with open(file=file.as_posix(), mode='wt') as file_object:
                json.dump(config, file_object)

            persipubsub.control.initialize_all_dbs(config_pth=file)

            queue = persipubsub.queue.Queue()
            queue.init(config_pth=file, queue_dir=tmp_dir.path / "queue")

            self.assertRaises(RuntimeError, queue.pop, sub_id=subscriber)

    def test_queue_initialisation(self):
        with temppathlib.TemporaryDirectory() as tmp_dir:
            config = tests.generate_test_config(path=tmp_dir.path)

            file = tmp_dir.path / "config.json"

            with open(file=file.as_posix(), mode='wt') as file_object:
                json.dump(config, file_object)

            persipubsub.control.initialize_all_dbs(config_pth=file)

            queue = persipubsub.queue.Queue()

            queue.init(config_pth=file, queue_dir=tmp_dir.path / "queue")

            queue_config = config['queues'][(tmp_dir.path / "queue").as_posix()]
            self.assertIsNotNone(queue.env)
            self.assertEqual(queue_config['path'], queue.env.path())
            hwm = queue_config['high-water-mark']
            self.assertEqual(hwm['HWM_LMDB_SIZE_BYTES'],
                             queue.hwm.hwm_lmdb_size_bytes)
            self.assertEqual(hwm['MAX_MSGS_NUM'], queue.hwm.max_msgs_num)
            self.assertEqual(hwm['MSG_TIMEOUT_SECS'],
                             queue.hwm.msg_timeout_secs)
            self.assertEqual(hwm['strategy'], queue.strategy.name)
            self.assertEqual(['sub'], queue.sub_list)

    def test_overflow_msgs_limit(self):
        with temppathlib.TemporaryDirectory() as tmp_dir:
            config = tests.generate_test_config(path=tmp_dir.path)

            file = tmp_dir.path / "config.json"

            with open(file=file.as_posix(), mode='wt') as file_object:
                json.dump(config, file_object)

            persipubsub.control.initialize_all_dbs(config_pth=file)

            queue = persipubsub.queue.Queue()

            queue.init(config_pth=file, queue_dir=tmp_dir.path / "queue")

            queue.hwm.max_msgs_num = tests.TEST_HWM_MSG_NUM

            msg = "hello world".encode(tests.ENCODING)

            self.assertEqual(0, queue.count_msgs())
            for _ in range(tests.TEST_HWM_MSG_NUM):
                queue.put(msg=msg, sub_list=queue.sub_list)

            self.assertEqual(tests.TEST_HWM_MSG_NUM, queue.count_msgs())

            queue.put(msg=msg, sub_list=queue.sub_list)

            self.assertEqual(
                int(tests.TEST_HWM_MSG_NUM - int(tests.TEST_HWM_MSG_NUM / 2)),
                queue.count_msgs())

    def test_overflow_limit_size(self):
        with temppathlib.TemporaryDirectory() as tmp_dir:
            config = tests.generate_test_config(path=tmp_dir.path)

            file = tmp_dir.path / "config.json"

            with open(file=file.as_posix(), mode='wt') as file_object:
                json.dump(config, file_object)

            persipubsub.control.initialize_all_dbs(config_pth=file)

            queue = persipubsub.queue.Queue()

            queue.init(config_pth=file, queue_dir=tmp_dir.path / "queue")

            queue.hwm.hwm_lmdb_size_bytes = tests.TEST_HWM_LMDB_SIZE

            msg = ("a" * (int(tests.LMDB_PAGE_SIZE / 4))).encode(tests.ENCODING)

            while queue.check_current_lmdb_size() <= tests.TEST_HWM_LMDB_SIZE:
                queue.put(msg=msg, sub_list=queue.sub_list)

            self.assertTrue(
                queue.check_current_lmdb_size() > tests.TEST_HWM_LMDB_SIZE)

            queue.put(msg=msg, sub_list=queue.sub_list)

            self.assertTrue(
                queue.check_current_lmdb_size() <= tests.TEST_HWM_LMDB_SIZE)

    def test_timeout(self):
        with temppathlib.TemporaryDirectory() as tmp_dir:
            config = tests.generate_test_config(path=tmp_dir.path)

            file = tmp_dir.path / "config.json"

            with open(file=file.as_posix(), mode='wt') as file_object:
                json.dump(config, file_object)

            persipubsub.control.initialize_all_dbs(config_pth=file)

            queue = persipubsub.queue.Queue()

            queue.init(config_pth=file, queue_dir=tmp_dir.path / "queue")
            queue.hwm.msg_timeout_secs = tests.TEST_MSG_TIMEOUT

            msg = "hello world".encode(tests.ENCODING)

            queue.put(msg=msg, sub_list=queue.sub_list)
            self.assertEqual(1, queue.count_msgs())
            queue.put(msg=msg, sub_list=queue.sub_list)
            self.assertEqual(2, queue.count_msgs())
            time.sleep(tests.TEST_MSG_TIMEOUT + 1)
            queue.put(msg=msg, sub_list=queue.sub_list)
            self.assertEqual(1, queue.count_msgs())

    def test_strategy_prune_first(self):
        with temppathlib.TemporaryDirectory() as tmp_dir:
            config = tests.generate_test_config(path=tmp_dir.path)

            file = tmp_dir.path / "config.json"

            with open(file=file.as_posix(), mode='wt') as file_object:
                json.dump(config, file_object)

            persipubsub.control.initialize_all_dbs(config_pth=file)

            queue = persipubsub.queue.Queue()

            queue.init(config_pth=file, queue_dir=tmp_dir.path / "queue")
            queue.strategy = persipubsub.queue.Strategy.prune_first

            queue.hwm.max_msgs_num = tests.TEST_HWM_MSG_NUM

            for index in range(tests.TEST_HWM_MSG_NUM):
                msg = "secret message {}".format(index).encode(tests.ENCODING)
                queue.put(msg=msg, sub_list=queue.sub_list)

            self.assertEqual("secret message 0".encode(tests.ENCODING),
                             queue.front(sub_id='sub'))

            msg = "secret message {}".format(tests.TEST_HWM_MSG_NUM).encode(
                tests.ENCODING)
            queue.put(msg=msg, sub_list=queue.sub_list)

            self.assertEqual(
                "secret message {}".format(
                    int((tests.TEST_HWM_MSG_NUM / 2) + 1)).encode(
                        tests.ENCODING), queue.front(sub_id='sub'))

    def test_strategy_prune_last(self):
        with temppathlib.TemporaryDirectory() as tmp_dir:
            config = tests.generate_test_config(path=tmp_dir.path)

            file = tmp_dir.path / "config.json"

            with open(file=file.as_posix(), mode='wt') as file_object:
                json.dump(config, file_object)

            persipubsub.control.initialize_all_dbs(config_pth=file)

            queue = persipubsub.queue.Queue()

            queue.init(config_pth=file, queue_dir=tmp_dir.path / "queue")
            queue.strategy = persipubsub.queue.Strategy.prune_last

            queue.hwm.max_msgs_num = tests.TEST_HWM_MSG_NUM

            for index in range(tests.TEST_HWM_MSG_NUM):
                msg = "secret message {}".format(index).encode(tests.ENCODING)
                queue.put(msg=msg, sub_list=queue.sub_list)

            self.assertEqual("secret message 0".encode(tests.ENCODING),
                             queue.front(sub_id='sub'))

            msg = "secret message {}".format(tests.TEST_HWM_MSG_NUM).encode(
                tests.ENCODING)
            queue.put(msg=msg, sub_list=queue.sub_list)

            self.assertEqual("secret message 0".encode(tests.ENCODING),
                             queue.front(sub_id='sub'))


if __name__ == '__main__':
    unittest.main()
