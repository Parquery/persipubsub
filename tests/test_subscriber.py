#!/usr/bin/env python
"""Test subscriber."""

import pathlib
import unittest
from typing import List

import temppathlib

import persipubsub.control
import persipubsub.queue
import persipubsub.subscriber
import tests

# pylint: disable=missing-docstring
# pylint: disable=protected-access


def setup(path: pathlib.Path,
          sub_list: List[str]) -> persipubsub.control.Control:
    """Create an initialized control"""
    control = persipubsub.control.Control(path=path)

    hwm = persipubsub.queue._HighWaterMark()
    strategy = persipubsub.queue._Strategy.prune_first

    control.init(
        subscriber_ids=sub_list,
        max_readers=tests.TEST_MAX_READER_NUM,
        max_size=tests.TEST_MAX_DB_SIZE_BYTES,
        high_watermark=hwm,
        strategy=strategy)

    return control


class TestSubscriber(unittest.TestCase):
    def test_receive_message(self):
        with temppathlib.TemporaryDirectory() as tmp_dir:
            _ = setup(path=tmp_dir.path, sub_list=['sub'])

            queue = persipubsub.queue._Queue()
            queue.init(path=tmp_dir.path)

            sub = persipubsub.subscriber.Subscriber()
            sub.init(sub_id="sub", path=tmp_dir.path)

            msg = "Hello World!".encode(tests.ENCODING)
            queue.put(msg=msg)

            with sub.receive(timeout=1) as received_msg:
                self.assertIsNotNone(received_msg)
                self.assertEqual(msg, received_msg)

    def test_timeout_subscriber(self):
        with temppathlib.TemporaryDirectory() as tmp_dir:
            _ = setup(path=tmp_dir.path, sub_list=['sub'])

            queue = persipubsub.queue._Queue()
            queue.init(path=tmp_dir.path)

            sub = persipubsub.subscriber.Subscriber()
            sub.init(sub_id='sub', path=tmp_dir.path)

            with sub.receive(timeout=1) as received_msg:
                self.assertIsNone(received_msg)
                msg = "message send after timeout and will not be popped" \
                      "".encode(tests.ENCODING)
                queue.put(msg=msg)
                self.assertIsNone(received_msg)

            self.assertEqual(
                "message send after timeout and will not be popped".encode(
                    tests.ENCODING), queue.front(sub_id='sub'))

            with sub.receive(timeout=1) as received_msg:
                self.assertIsNotNone(received_msg)
                self.assertEqual(
                    "message send after timeout and will not be popped".encode(
                        tests.ENCODING), received_msg)

            self.assertIsNone(queue.front(sub_id='sub'))

    def test_pop(self):
        with temppathlib.TemporaryDirectory() as tmp_dir:
            _ = setup(path=tmp_dir.path, sub_list=['sub'])

            queue = persipubsub.queue._Queue()
            queue.init(path=tmp_dir.path)

            sub = persipubsub.subscriber.Subscriber()
            sub.init(sub_id='sub', path=tmp_dir.path)

            msg1 = "I'm a message".encode(tests.ENCODING)
            queue.put(msg=msg1)

            msg2 = "I'm a message too".encode(tests.ENCODING)
            queue.put(msg=msg2)

            sub._pop()

            with sub.receive() as msg:
                self.assertIsNotNone(msg)
                self.assertEqual(msg2, msg)

    def test_pop_when_empty(self):
        with temppathlib.TemporaryDirectory() as tmp_dir:
            _ = setup(path=tmp_dir.path, sub_list=['sub'])

            queue = persipubsub.queue._Queue()
            queue.init(path=tmp_dir.path)

            sub = persipubsub.subscriber.Subscriber()
            sub.init(sub_id='sub', path=tmp_dir.path)

            self.assertRaises(RuntimeError, sub._pop)

    def test_receive_to_top(self):
        with temppathlib.TemporaryDirectory() as tmp_dir:
            _ = setup(path=tmp_dir.path, sub_list=['sub'])

            queue = persipubsub.queue._Queue()
            queue.init(path=tmp_dir.path)

            sub = persipubsub.subscriber.Subscriber()
            sub.init(sub_id='sub', path=tmp_dir.path)

            msg1 = "I'm a message".encode(tests.ENCODING)
            queue.put(msg=msg1)

            msg2 = "I'm a message too".encode(tests.ENCODING)
            queue.put(msg=msg2)

            with sub.receive_to_top() as msg:
                self.assertIsNotNone(msg)
                self.assertEqual(msg2, msg)


if __name__ == '__main__':
    unittest.main()
