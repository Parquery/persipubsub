#!/usr/bin/env python
"""Set default values of persipubsub and offers encoding tools."""
from typing import Set

import lmdb

# define all encodings here
#: Default encoding from str to bytes
ENCODING = 'utf-8'  # type: str
#: Default bytes length for encoding from int to bytes
BYTES_LENGTH = 8  # type: int
#: Default bytes order for encoding from int to bytes
BYTES_ORDER = 'big'  # type: str

# Define all database names here
#: The data database the actual data.
#: The data is stored in the following (key | value) pair: (msg_id | data)
DATA_DB = "data_db".encode(ENCODING)
#: The pending database stores how many subscriber still need to read a message.
#: The data is stored in the following (key | value) pair:
#: (msg_id | pending subscriber)
PENDING_DB = "pending_db".encode(ENCODING)
#: The meta database stores the timestamp when the message was sent.
#: The data is stored in the following (key | value) pair: (msg_id | timestamp)
META_DB = "meta_db".encode(ENCODING)
#: The queue database stores the high water mark and pruning strategy.
#: The data is stored in the following (key | value) pair:
#: (queue_pth | all queue data)
QUEUE_DB = "queue_db".encode(ENCODING)
#: The subscriber database stores the subscriber ids of the queue.
#: The data is stored in the following (key | value) pair: (subscriber_id | -)
SUBSCRIBER_DB = "subscriber_db".encode(ENCODING)

# queues default
#: Default maximal LMDB size (in bytes)
DEFAULT_MAX_DB_SIZE = 32 * 1024**3  # type: int
#: Default maximal readers
DEFAULT_MAX_READERS = 1024  # type: int
#: default maximal named databases
DEFAULT_MAX_DB_NUM = 1024  # type: int

# queues default keys
MAX_DB_SIZE_KEY = "max_db_size".encode(ENCODING)
MAX_READERS_KEY = "max_readers".encode(ENCODING)
MAX_DB_NUM_KEY = "max_db_num".encode(ENCODING)
HWM_DB_SIZE_KEY = "hwm_db_size".encode(ENCODING)
MAX_MESSAGES_KEY = "max_messages".encode(ENCODING)
MESSAGE_TIMEOUT_KEY = "message_timeout".encode(ENCODING)
STRATEGY_KEY = "strategy".encode(ENCODING)


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
    """Hold initialization data for queue."""

    def __init__(self, message_timeout: int, max_messages: int,
                 hwm_db_size: int, strategy: str,
                 subscriber_ids: Set[str]) -> None:
        """
        Initialize with the given values.

        :param message_timeout:
            time after which msg is classified as dangling msg (secs)
        :param max_messages: maximal amount of msg
        :param hwm_db_size: high water mark for total size of LMDB (in bytes)
        :param strategy: pruning strategy
        :param subscriber_ids: List of subscribers
        """
        # pylint: disable=too-many-arguments
        self.message_timeout = message_timeout
        self.max_messages = max_messages
        self.hwm_db_size = hwm_db_size
        self.strategy = strategy
        self.subscriber_ids = subscriber_ids


def retrieve_queue_data(env: lmdb.Environment) -> QueueData:
    """
    Retrieve set value in the queue database.

    Value stored in the queue database are high water mark values and pruning
    strategy.

    :param env: environment that stores queue data
    :return: data stored in LMDB related to the queue
    """
    with env.begin(write=False) as txn:
        queue_db = env.open_db(key=QUEUE_DB, txn=txn, create=False)
        message_timeout = bytes_to_int(
            txn.get(key=MESSAGE_TIMEOUT_KEY, db=queue_db))
        max_messages = bytes_to_int(txn.get(key=MAX_MESSAGES_KEY, db=queue_db))
        hwm_db_size = bytes_to_int(txn.get(key=HWM_DB_SIZE_KEY, db=queue_db))
        strategy = bytes_to_str(txn.get(key=STRATEGY_KEY, db=queue_db))

        subscriber_db = env.open_db(key=SUBSCRIBER_DB, txn=txn, create=False)
        sub_cursor = txn.cursor(db=subscriber_db)
        subscriber_ids = set(
            bytes_to_str(subscriber_id)
            for subscriber_id in sub_cursor.iternext(values=False))

    queue_data = QueueData(
        message_timeout=message_timeout,
        max_messages=max_messages,
        hwm_db_size=hwm_db_size,
        strategy=strategy,
        subscriber_ids=subscriber_ids)
    return queue_data
