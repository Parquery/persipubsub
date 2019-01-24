#!/usr/bin/env python
"""Test subscriber."""

import json
import unittest

import temppathlib

import messages.message_pb2
import persipubsub.control
import persipubsub.queue
import persipubsub.subscriber
import tests

# pylint: disable=missing-docstring


class TestSubscriber(unittest.TestCase):
    # TODO(snaji): receive from cache or fetch from lmdb
    def test_receive_message(self):
        with temppathlib.TemporaryDirectory() as tmp_dir:
            config = tests.generate_test_config(path=tmp_dir.path)

            file = tmp_dir.path / "config.json"

            with open(file=file.as_posix(), mode='wt') as file_object:
                json.dump(config, file_object)

            persipubsub.control.initialize_all_dbs(config_pth=file)
            queue = persipubsub.queue.Queue()
            queue.init(config_pth=file, queue_dir=tmp_dir.path / "queue")

            sub = persipubsub.subscriber.Sub()
            sub.init(sub_id="sub", config_pth=file)

            msg = messages.message_pb2.SomeMessage()
            msg.id = 1
            msg.text = "Hello World!"
            queue.put(msg=msg.SerializeToString(), sub_list=["sub"])

            with sub.receive(timeout=1) as received_msg:
                self.assertIsNotNone(received_msg)
                self.assertEqual(msg.SerializeToString(), received_msg)

    def test_timeout_subscriber(self):
        with temppathlib.TemporaryDirectory() as tmp_dir:
            config = tests.generate_test_config(path=tmp_dir.path)

            file = tmp_dir.path / "config.json"

            with open(file=file.as_posix(), mode='wt') as file_object:
                json.dump(config, file_object)

            persipubsub.control.initialize_all_dbs(config_pth=file)
            queue = persipubsub.queue.Queue()
            queue.init(config_pth=file, queue_dir=tmp_dir.path / "queue")

            sub = persipubsub.subscriber.Sub()
            sub.init(sub_id='sub', config_pth=file)

            with sub.receive(timeout=1) as received_msg:
                self.assertIsNone(received_msg)
                msg = "message send after timeout and will not be popped" \
                      "".encode(tests.ENCODING)
                queue.put(msg=msg, sub_list=queue.sub_list)
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
            config = tests.generate_test_config(path=tmp_dir.path)

            file = tmp_dir.path / "config.json"

            with open(file=file.as_posix(), mode='wt') as file_object:
                json.dump(config, file_object)

            persipubsub.control.initialize_all_dbs(config_pth=file)
            queue = persipubsub.queue.Queue()
            queue.init(config_pth=file, queue_dir=tmp_dir.path / "queue")

            sub = persipubsub.subscriber.Sub()
            sub.init(sub_id='sub', config_pth=file)

            # TODO(snaji): protobuf or str example
            msg1 = "I'm a message".encode(tests.ENCODING)
            queue.put(msg=msg1, sub_list=queue.sub_list)

            msg2 = "I'm a message too".encode(tests.ENCODING)
            queue.put(msg=msg2, sub_list=queue.sub_list)

            sub.pop()

            with sub.receive() as msg:
                self.assertIsNotNone(msg)
                self.assertEqual(msg2, msg)

    def test_pop_when_empty(self):
        with temppathlib.TemporaryDirectory() as tmp_dir:
            config = tests.generate_test_config(path=tmp_dir.path)

            file = tmp_dir.path / "config.json"

            with open(file=file.as_posix(), mode='wt') as file_object:
                json.dump(config, file_object)

            persipubsub.control.initialize_all_dbs(config_pth=file)
            queue = persipubsub.queue.Queue()
            queue.init(config_pth=file, queue_dir=tmp_dir.path / "queue")

            sub = persipubsub.subscriber.Sub()
            sub.init(sub_id='sub', config_pth=file)

            self.assertRaises(RuntimeError, sub.pop)


if __name__ == '__main__':
    unittest.main()
