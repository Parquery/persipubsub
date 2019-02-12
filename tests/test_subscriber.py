#!/usr/bin/env python
"""Test subscriber."""

import unittest
from typing import List

import temppathlib

import persipubsub.control
import persipubsub.environment
import persipubsub.queue
import persipubsub.subscriber
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


class TestSubscriber(unittest.TestCase):
    def test_receive_message(self) -> None:
        with temppathlib.TemporaryDirectory() as tmp_dir:
            env = persipubsub.environment.Environment(path=tmp_dir.path)

            subscriber = 'sub'
            _ = setup(env=env, sub_list=[subscriber])

            sub = env.new_subscriber(identifier=subscriber)
            queue = env.new_publisher().queue
            assert isinstance(queue, persipubsub.queue._Queue)

            msg = "Hello World!".encode(tests.ENCODING)
            queue.put(msg=msg)

            with sub.receive(timeout=1) as received_msg:
                self.assertIsNotNone(received_msg)
                self.assertEqual(msg, received_msg)

    def test_timeout_subscriber(self) -> None:
        with temppathlib.TemporaryDirectory() as tmp_dir:
            env = persipubsub.environment.Environment(path=tmp_dir.path)

            subscriber = 'sub'
            _ = setup(env=env, sub_list=[subscriber])

            sub = env.new_subscriber(identifier=subscriber)
            queue = env.new_publisher().queue
            assert isinstance(queue, persipubsub.queue._Queue)

            with sub.receive(timeout=1) as received_msg:
                self.assertIsNone(received_msg)
                msg = "message send after timeout and will not be popped" \
                      "".encode(tests.ENCODING)
                queue.put(msg=msg)
                self.assertIsNone(received_msg)

            self.assertEqual(
                "message send after timeout and will not be popped".encode(
                    tests.ENCODING), queue.front(identifier=subscriber))

            with sub.receive(timeout=1) as received_msg:
                self.assertIsNotNone(received_msg)
                self.assertEqual(
                    "message send after timeout and will not be popped".encode(
                        tests.ENCODING), received_msg)

            self.assertIsNone(queue.front(identifier=subscriber))

    def test_pop(self) -> None:
        with temppathlib.TemporaryDirectory() as tmp_dir:
            env = persipubsub.environment.Environment(path=tmp_dir.path)

            subscriber = 'sub'
            _ = setup(env=env, sub_list=[subscriber])

            sub = env.new_subscriber(identifier=subscriber)
            queue = env.new_publisher().queue
            assert isinstance(queue, persipubsub.queue._Queue)

            msg1 = "I'm a message".encode(tests.ENCODING)
            queue.put(msg=msg1)

            msg2 = "I'm a message too".encode(tests.ENCODING)
            queue.put(msg=msg2)

            sub._pop()

            with sub.receive() as msg:
                self.assertIsNotNone(msg)
                self.assertEqual(msg2, msg)

    def test_pop_when_empty(self) -> None:
        with temppathlib.TemporaryDirectory() as tmp_dir:
            env = persipubsub.environment.Environment(path=tmp_dir.path)

            subscriber = 'sub'
            _ = setup(env=env, sub_list=[subscriber])

            sub = env.new_subscriber(identifier=subscriber)

            self.assertRaises(RuntimeError, sub._pop)

    def test_receive_to_top(self) -> None:
        with temppathlib.TemporaryDirectory() as tmp_dir:
            env = persipubsub.environment.Environment(path=tmp_dir.path)

            subscriber = 'sub'
            _ = setup(env=env, sub_list=[subscriber])

            sub = env.new_subscriber(identifier=subscriber)
            queue = env.new_publisher().queue
            assert isinstance(queue, persipubsub.queue._Queue)

            msg1 = "I'm a message".encode(tests.ENCODING)
            queue.put(msg=msg1)

            msg2 = "I'm a message too".encode(tests.ENCODING)
            queue.put(msg=msg2)

            with sub.receive_to_top() as msg:
                self.assertIsNotNone(msg)
                self.assertEqual(msg2, msg)


if __name__ == '__main__':
    unittest.main()
