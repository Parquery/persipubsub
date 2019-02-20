#!/usr/bin/env python
"""Set default values of persipubsub and offers encoding tools."""
from typing import List, Optional

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


def str_to_bytes(string: str) -> bytes:
    """
    Encode a string with utf-8 encoding.

    :param string: any string
    :return: string encoded with utf-8
    """
    return string.encode(encoding=ENCODING)


def bytes_to_str(encoded_str: bytes) -> str:
    """
    Decode bytes with utf-8 encoding.

    :param encoded_str: any bytes
    :return: bytes decoded with utf-8
    """
    return encoded_str.decode(encoding=ENCODING)


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


class QueueData:
    """Hold queue data."""

    def __init__(self, msg_timeout_secs: int, max_msgs_num: int,
                 hwm_db_size_bytes: int, strategy: str,
                 subscriber_ids: List[str]) -> None:
        """
        Initialize.

        :param msg_timeout_secs: time after which msg is classified as dangling
            msg (secs)
        :param max_msgs_num: maximal amount of msg
        :param hwm_db_size_bytes: high water mark for total size of LMDB (bytes)
        :param strategy: pruning strategy
        :param subscriber_ids: List of subscribers
        """
        # pylint: disable=too-many-arguments
        self.msg_timeout_secs = msg_timeout_secs
        self.max_msgs_num = max_msgs_num
        self.hwm_db_size_bytes = hwm_db_size_bytes
        self.strategy = strategy
        self.subscriber_ids = subscriber_ids


def lookup_queue_data(env: lmdb.Environment) -> QueueData:
    """
    Lookup set value in named database 'queue_db'.

    Value stored in 'queue_db' are high water mark values and pruning strategy.

    :param env: environment that stores queue data
    :return: lookup result
    """
    with env.begin(write=False) as txn:
        queue_db = env.open_db(key=QUEUE_DB, txn=txn, create=False)
        msg_timeout_secs_bytes = txn.get(key=MSG_TIMEOUT_SECS_KEY, db=queue_db)
        max_msgs_num_bytes = txn.get(key=MAX_MSGS_NUM_KEY, db=queue_db)
        hwm_db_size_bytes = txn.get(key=HWM_DB_SIZE_BYTES_KEY, db=queue_db)
        strategy_bytes = txn.get(key=STRATEGY_KEY, db=queue_db)
        subscriber_ids_bytes = txn.get(key=SUBSCRIBER_IDS_KEY, db=queue_db)

    msg_timeout_secs = bytes_to_int(msg_timeout_secs_bytes)
    max_msgs_num = bytes_to_int(max_msgs_num_bytes)
    hwm_db_size = bytes_to_int(hwm_db_size_bytes)

    strategy = bytes_to_str(encoded_str=strategy_bytes)

    if subscriber_ids_bytes is None:
        subscriber_ids = []  # type: List[str]
    else:
        subscriber_ids = bytes_to_str(
            encoded_str=subscriber_ids_bytes).split(' ')

    queue_data = QueueData(
        msg_timeout_secs=msg_timeout_secs,
        max_msgs_num=max_msgs_num,
        hwm_db_size_bytes=hwm_db_size,
        strategy=strategy,
        subscriber_ids=subscriber_ids)
    return queue_data
