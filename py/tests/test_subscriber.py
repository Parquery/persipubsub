#!/usr/bin/env python
"""Test subscriber."""

import unittest
from typing import Set

import temppathlib

import persipubsub.control
import persipubsub.environment
import persipubsub.queue
import persipubsub.subscriber
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


class TestSubscriber(unittest.TestCase):
    def test_receive_message(self) -> None:
        with temppathlib.TemporaryDirectory() as tmp_dir:
            env = persipubsub.environment.Environment(path=tmp_dir.path)

            subscriber = 'sub'
            _ = setup(env=env, sub_set={subscriber})

            sub = env.new_subscriber(identifier=subscriber)
            queue = env.new_publisher().queue
            assert queue is not None

            msg = "Hello World!".encode(tests.ENCODING)
            queue.put(msg=msg)

            with sub.receive(timeout=1) as received_msg:
                self.assertIsNotNone(received_msg)
                self.assertEqual(msg, received_msg)

    def test_timeout_subscriber(self) -> None:
        with temppathlib.TemporaryDirectory() as tmp_dir:
            env = persipubsub.environment.Environment(path=tmp_dir.path)

            subscriber = 'sub'
            _ = setup(env=env, sub_set={subscriber})

            sub = env.new_subscriber(identifier=subscriber)
            queue = env.new_publisher().queue
            assert queue is not None

            with sub.receive(timeout=1) as received_msg:
                self.assertIsNone(received_msg)
                msg = "message send after timeout and will not be popped" \
                      "".encode(tests.ENCODING)
                queue.put(msg=msg)
                self.assertIsNone(received_msg)

            _, received_msg = queue.front(sub_id=subscriber)

            self.assertEqual(
                "message send after timeout and will not be popped".encode(
                    tests.ENCODING), received_msg)

            with sub.receive(timeout=1) as received_msg:
                self.assertIsNotNone(received_msg)
                self.assertEqual(
                    "message send after timeout and will not be popped".encode(
                        tests.ENCODING), received_msg)

            _, received_msg = queue.front(sub_id=subscriber)
            self.assertIsNone(received_msg)

    def test_pop(self) -> None:
        with temppathlib.TemporaryDirectory() as tmp_dir:
            env = persipubsub.environment.Environment(path=tmp_dir.path)

            subscriber = 'sub'
            _ = setup(env=env, sub_set={subscriber})

            sub = env.new_subscriber(identifier=subscriber)
            queue = env.new_publisher().queue
            assert queue is not None

            msg1 = "I'm a message".encode(tests.ENCODING)
            queue.put(msg=msg1)

            msg2 = "I'm a message too".encode(tests.ENCODING)
            queue.put(msg=msg2)

            msg_id, _ = queue.front(sub_id=subscriber)
            assert msg_id is not None
            sub._pop(msg_id=msg_id)

            with sub.receive() as msg:
                self.assertIsNotNone(msg)
                self.assertEqual(msg2, msg)

    def test_pop_when_empty(self) -> None:
        with temppathlib.TemporaryDirectory() as tmp_dir:
            env = persipubsub.environment.Environment(path=tmp_dir.path)

            subscriber = 'sub'
            _ = setup(env=env, sub_set={subscriber})

            sub = env.new_subscriber(identifier=subscriber)

            self.assertRaises(
                RuntimeError, sub._pop, msg_id="".encode(tests.ENCODING))

    def test_receive_to_top(self) -> None:
        with temppathlib.TemporaryDirectory() as tmp_dir:
            env = persipubsub.environment.Environment(path=tmp_dir.path)

            subscriber = 'sub'
            _ = setup(env=env, sub_set={subscriber})

            sub = env.new_subscriber(identifier=subscriber)
            queue = env.new_publisher().queue
            assert queue is not None

            msg1 = "I'm a message".encode(tests.ENCODING)
            queue.put(msg=msg1)

            msg2 = "I'm a message too".encode(tests.ENCODING)
            queue.put(msg=msg2)

            with sub.receive_to_top() as msg:
                self.assertIsNotNone(msg)
                self.assertEqual(msg2, msg)


if __name__ == '__main__':
    unittest.main()
