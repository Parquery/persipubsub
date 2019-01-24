#!/usr/bin/env python3
"""Test persipubsub."""

import json
import pathlib
import unittest
from typing import Any, Dict

import temppathlib

import persipubsub

# pylint: disable=missing-docstring

# TODO(snaji): test init.py

LMDB_PAGE_SIZE = 4096

TEST_MSG_TIMEOUT = 1  # type: int
TEST_HWM_MSG_NUM = 10  # type: int
TEST_HWM_LMDB_SIZE = LMDB_PAGE_SIZE * 2  # type: int

# define all encodings here
ENCODING = 'utf-8'
BYTES_LENGTH = 8
BYTES_ORDER = 'big'

# define all database names here
DATA_DB = "data_db".encode(ENCODING)  # msg_id | data
PENDING_DB = "pending_db".encode(ENCODING)  # msg_id | pending subscriber
META_DB = "meta_db".encode(ENCODING)  # msg_id | metadata


def generate_test_config(path: pathlib.Path) -> Dict[str, Any]:
    queue_dir = path / "queue"
    queue_dir.mkdir(parents=True, exist_ok=True)

    test_config = {
        "pub": {
            "out_queue": queue_dir.as_posix(),
            "subscribers": ["sub"]
        },
        "sub": {
            "in_queue": queue_dir.as_posix()
        },
        "queues": {
            queue_dir.as_posix(): {
                "path": queue_dir.as_posix(),
                "max_reader_num": 1024,
                "max_db_num": 1024,
                "max_db_size_bytes": 34359738368,
                "subscribers": ["sub"],
                "high-water-mark": {
                    "MSG_TIMEOUT_SECS": TEST_MSG_TIMEOUT,
                    "MAX_MSGS_NUM": TEST_HWM_MSG_NUM,
                    "HWM_LMDB_SIZE_BYTES": TEST_HWM_LMDB_SIZE,
                    "strategy": "prune_first"
                }
            },
        }
    }

    return test_config


class TestPersiPubSub(unittest.TestCase):
    def test_get_config(self):
        with temppathlib.TemporaryDirectory() as tmp_dir:
            expected_config = generate_test_config(path=tmp_dir.path)

            file = tmp_dir.path / "config.json"

            with open(file=file.as_posix(), mode='wt') as file_object:
                json.dump(expected_config, file_object)

            config = persipubsub.get_config(path=file)

            self.assertDictEqual(expected_config, config)


if __name__ == '__main__':
    unittest.main()
