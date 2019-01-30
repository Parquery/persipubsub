#!/usr/bin/env python
"""Test publisher."""

import json
import unittest

import lmdb
import temppathlib

import persipubsub.control
import persipubsub.publisher
import persipubsub.queue
import tests

# pylint: disable=missing-docstring
# pylint: disable=protected-access


class TestPublisher(unittest.TestCase):
    def test_send(self):
        # pylint: disable=too-many-locals
        with temppathlib.TemporaryDirectory() as tmp_dir:
            config = tests.generate_test_config(path=tmp_dir.path)

            file = tmp_dir.path / "config.json"

            with open(file=file.as_posix(), mode='wt') as file_object:
                json.dump(config, file_object)

            persipubsub.control.initialize_all_dbs(config_pth=file)
            queue = persipubsub.queue._Queue()
            queue.init(config_pth=file, queue_dir=tmp_dir.path / "queue")

            pub = persipubsub.publisher.Pub()
            pub.init(pub_id="pub", config_pth=file)

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

    def test_send_to_nonexisting_db(self):
        with temppathlib.TemporaryDirectory() as tmp_dir:
            config = tests.generate_test_config(path=tmp_dir.path)

            file = tmp_dir.path / "config.json"

            with open(file=file.as_posix(), mode='wt') as file_object:
                json.dump(config, file_object)

            persipubsub.control.initialize_all_dbs(config_pth=file)
            queue = persipubsub.queue._Queue()
            queue.init(config_pth=file, queue_dir=tmp_dir.path / "queue")

            pub = persipubsub.publisher.Pub()
            pub.init(pub_id="pub", config_pth=file)

            pub.sub_list.append('non-existent sub')

            msg = "message for every subscriber".encode(tests.ENCODING)
            self.assertRaises(lmdb.NotFoundError, pub.send, msg=msg)

    def test_send_many(self):
        # pylint: disable=too-many-locals
        with temppathlib.TemporaryDirectory() as tmp_dir:
            config = tests.generate_test_config(path=tmp_dir.path)

            file = tmp_dir.path / "config.json"

            with open(file=file.as_posix(), mode='wt') as file_object:
                json.dump(config, file_object)

            persipubsub.control.initialize_all_dbs(config_pth=file)

            pub = persipubsub.publisher.Pub()
            pub.init(pub_id='pub', config_pth=file)

            msg = "I'm a message".encode(tests.ENCODING)
            msgs = []
            msg_num = 10
            for _ in range(msg_num):
                msgs.append(msg)

            subscriber = "sub"

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
