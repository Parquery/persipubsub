#!/usr/bin/env python
"""Test publisher."""

import pathlib
import unittest
from typing import List

import temppathlib

import persipubsub.control
import persipubsub.publisher
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


class TestPublisher(unittest.TestCase):
    def test_send(self):
        # pylint: disable=too-many-locals
        with temppathlib.TemporaryDirectory() as tmp_dir:
            _ = setup(path=tmp_dir.path, sub_list=['sub'])

            queue = persipubsub.queue._Queue()
            queue.init(path=tmp_dir.path)

            pub = persipubsub.publisher.Publisher()
            pub.init(path=tmp_dir.path)

            msg = "Hello world!".encode(tests.ENCODING)
            pub.send(msg=msg)

            subscriber = "sub".encode(tests.ENCODING)
            with queue.env.begin(write=False) as txn:
                self.assertIsNotNone(txn.get(key=subscriber))
                sub_db = queue.env.open_db(
                    key=subscriber, txn=txn, create=False)
                cursor = txn.cursor(db=sub_db)
                self.assertTrue(cursor.first())

                key = cursor.key()

                data_db = queue.env.open_db(
                    key=tests.DATA_DB, txn=txn, create=False)
                item = txn.get(key=key, db=data_db)
                self.assertIsNotNone(item)
                self.assertEqual(msg, item)

    def test_send_many(self):
        # pylint: disable=too-many-locals
        with temppathlib.TemporaryDirectory() as tmp_dir:
            subscriber = "sub"
            _ = setup(path=tmp_dir.path, sub_list=[subscriber])

            pub = persipubsub.publisher.Publisher()
            pub.init(path=tmp_dir.path)

            msg = "I'm a message".encode(tests.ENCODING)
            msgs = []
            msg_num = 10
            for _ in range(msg_num):
                msgs.append(msg)

            pub.send_many(msgs=msgs)

            with pub.queue.env.begin(write=False) as txn:
                self.assertIsNotNone(
                    txn.get(key=subscriber.encode(tests.ENCODING)))

                sub_db = pub.queue.env.open_db(
                    key=subscriber.encode(tests.ENCODING),
                    txn=txn,
                    create=False)

                sub_stat = txn.stat(db=sub_db)
                self.assertEqual(msg_num, sub_stat['entries'])

                data_db = pub.queue.env.open_db(
                    key=tests.DATA_DB, txn=txn, create=False)

                data_stat = txn.stat(db=data_db)
                self.assertEqual(msg_num, data_stat['entries'])


if __name__ == '__main__':
    unittest.main()
