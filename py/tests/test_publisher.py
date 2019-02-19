#!/usr/bin/env python
"""Test publisher."""

import unittest
from typing import List

import lmdb
import temppathlib

import persipubsub.control
import persipubsub.environment
import persipubsub.queue
import tests

# pylint: disable=missing-docstring
# pylint: disable=protected-access


def setup(env: persipubsub.environment.Environment,
          sub_list: List[str]) -> persipubsub.control.Control:
    """Create an initialized control"""
    hwm = persipubsub.queue.HighWaterMark()
    strategy = persipubsub.queue.Strategy.prune_first

    control = env.new_control(
        subscriber_ids=sub_list, high_watermark=hwm, strategy=strategy)

    return control


class TestPublisher(unittest.TestCase):
    def test_send(self) -> None:
        # pylint: disable=too-many-locals
        with temppathlib.TemporaryDirectory() as tmp_dir:
            env = persipubsub.environment.Environment(path=tmp_dir.path)
            _ = setup(env=env, sub_list=['sub'])

            pub = env.new_publisher()

            msg = "Hello world!".encode(tests.ENCODING)
            pub.send(msg=msg)

            subscriber = "sub".encode(tests.ENCODING)

            with env.env.begin(write=False) as txn:
                self.assertIsNotNone(txn.get(key=subscriber))
                sub_db = env.env.open_db(key=subscriber, txn=txn, create=False)
                cursor = txn.cursor(db=sub_db)
                self.assertTrue(cursor.first())

                key = cursor.key()

                data_db = env.env.open_db(
                    key=tests.DATA_DB, txn=txn, create=False)
                item = txn.get(key=key, db=data_db)
                self.assertIsNotNone(item)
                self.assertEqual(msg, item)

    def test_send_many(self) -> None:
        # pylint: disable=too-many-locals
        with temppathlib.TemporaryDirectory() as tmp_dir:
            env = persipubsub.environment.Environment(path=tmp_dir.path)
            subscriber = "sub"
            _ = setup(env=env, sub_list=[subscriber])

            pub = env.new_publisher()

            msg = "I'm a message".encode(tests.ENCODING)
            msgs = []
            msg_num = 10
            for _ in range(msg_num):
                msgs.append(msg)

            pub.send_many(msgs=msgs)

            assert isinstance(pub.queue, persipubsub.queue._Queue)
            assert isinstance(pub.queue.env, lmdb.Environment)
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
