#!/usr/bin/env python
"""Distribute messages persistent from publisher to subscriber."""
import json
import pathlib
from typing import Any, Dict, Union

import lmdb

# define all encodings here
ENCODING = 'utf-8'  # type: str
BYTES_LENGTH = 8  # type: int
BYTES_ORDER = 'big'  # type: str

# define all database names here
DATA_DB = "data_db".encode(ENCODING)  # msg_id | data
PENDING_DB = "pending_db".encode(ENCODING)  # msg_id | pending subscriber
META_DB = "meta_db".encode(ENCODING)  # msg_id | metadata
QUEUE_DB = "queue_db".encode(ENCODING)  # queue_pth | all queue data

# queues default
MAX_DB_SIZE_BYTES = 32 * 1024**3  # type: int
MAX_READER_NUM = 1024  # type: int
MAX_DB_NUM = 1024  # type: int

# queues default keys
MAX_DB_SIZE_BYTES_KEY = "max_db_size_bytes".encode(ENCODING)
MAX_READER_NUM_KEY = "max_reader_num".encode(ENCODING)
MAX_DB_NUM_KEY = "max_db_num".encode(ENCODING)
HWM_DB_SIZE_BYTES_KEY = "hwm_db_size_bytes".encode(ENCODING)
MAX_MSGS_NUM_KEY = "max_msgs_num".encode(ENCODING)
MSG_TIMEOUT_SECS_KEY = "msg_timeout_secs".encode(ENCODING)
STRATEGY_KEY = "strategy".encode(ENCODING)
SUBSCRIBER_IDS_KEY = "subscriber_ids".encode(ENCODING)


def encoding(string: str) -> bytes:
    """
    Encode a string with utf-8 encoding.

    :param string: any string
    :return: string encoded as utf-8
    """
    return string.encode(encoding=ENCODING)


def int_to_bytes(value: int) -> bytes:
    """
    Encode an integer to an array of bytes.

    :param value: any integer
    :return: integer value representation as bytes
    """
    return value.to_bytes(length=BYTES_LENGTH, byteorder=BYTES_ORDER)


def bytes_to_int(array_of_bytes: bytes) -> int:
    """
    Decode an array of bytes to an integer.

    :param array_of_bytes: any array of bytes
    :return: array of bytes representation as integer value
    """
    return int.from_bytes(bytes=array_of_bytes, byteorder=BYTES_ORDER)


def get_queue_data(path: pathlib.Path, key: bytes) -> bytes:
    """
    Get queue data.

    :param path: to the queue
    :param key: for lookup
    :return: lookup result
    """
    env = lmdb.open(
        path=path.as_posix(),
        map_size=MAX_DB_SIZE_BYTES,
        subdir=True,
        max_readers=MAX_READER_NUM,
        max_dbs=MAX_READER_NUM,
        max_spare_txns=0)

    with env.begin(write=False) as txn:
        queue_db = env.open_db(key=QUEUE_DB, txn=txn, create=False)
        data = txn.get(key=key, db=queue_db)

    return data


def put_queue_data(path: pathlib.Path, key: bytes, value: bytes) -> None:
    """
    Get queue data.

    :param path: to the queue
    :param key: of value to store
    :param value: to store
    :return:
    """
    env = lmdb.open(
        path=path,
        map_size=MAX_DB_SIZE_BYTES,
        subdir=True,
        max_readers=MAX_READER_NUM,
        max_dbs=MAX_READER_NUM,
        max_spare_txns=0)

    with env.begin(write=True) as txn:
        queue_db = env.open_db(key=QUEUE_DB, txn=txn, create=False)
        txn.put(key=key, value=value, db=queue_db)
