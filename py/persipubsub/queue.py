#!/usr/bin/env python3
"""Store messages in a local LMDB."""
import datetime
import enum
import pathlib
import uuid
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import icontract
import lmdb

import persipubsub
import persipubsub.database

#: Default message timeout (secs)
DEFAULT_MESSAGE_TIMEOUT = 500  # type: int
#: Default maximal messages
DEFAULT_MAX_MESSAGES = 1024 * 64
#: Default high water mark LMDB size (in bytes)
DEFAULT_HWM_DB_SIZE = 30 * 1024**3  # type: int


class Strategy(enum.Enum):
    """Hold possible strategies."""

    PRUNE_FIRST = 0
    PRUNE_LAST = 1


def _parse_strategy(identifier: str) -> Strategy:
    """
    Parse overflow strategy.

    :param identifier: Strategy stored in LMDB
    :return: set overflow strategy
    """
    if Strategy.PRUNE_FIRST.name == identifier:
        return Strategy.PRUNE_FIRST

    if Strategy.PRUNE_LAST.name == identifier:
        return Strategy.PRUNE_LAST

    raise ValueError("Unknown strategy: '{}' choosen.".format(identifier))


class HighWaterMark:
    """Hold high water mark limits."""

    def __init__(self,
                 message_timeout: Optional[int] = None,
                 max_messages: Optional[int] = None,
                 hwm_db_size: Optional[int] = None) -> None:
        """
        Initialize with given values.

        :param message_timeout:
            time after which msg is classified as dangling msg (secs)
        :param max_messages: maximal amount of msg
        :param hwm_db_size:
            high water mark for total size of LMDB (in bytes)
        """
        self.message_timeout = DEFAULT_MESSAGE_TIMEOUT \
            if message_timeout is None else message_timeout  # type: int
        self.max_messages = DEFAULT_MAX_MESSAGES \
            if max_messages is None else max_messages  # type: int
        self.hwm_lmdb_size = DEFAULT_HWM_DB_SIZE \
            if hwm_db_size is None else hwm_db_size  # type: int


def _initialize_environment(
        queue_dir: pathlib.Path,
        max_reader_num: int = 1024,
        max_db_num: int = 1024,
        max_db_size_bytes: int = 32 * 1024**3) -> lmdb.Environment:
    """
    Initialize the queue; the queue directory is assumed to exist.

    | [https://lmdb.readthedocs.io/en/release/#lmdb.Environment]
    | Restrictions set when opening LMDB.
    | path: Location of directory (if subdir=True) or file prefix to store the
    | database.
    | map_size: Maximum size database may grow to; used to size the memory
    | mapping. If database grows larger than map_size, an exception will be
    | raised and the user must close and reopen Environment. On 64-bit there is
    | no penalty for making this huge (say 1TB). Must be <2GB on 32-bit.
    | subdir: If True, path refers to a subdirectory to store the data and lock
    | files in, otherwise it refers to a filename prefix.
    | max_readers: Maximum number of simultaneous read transactions. Can only
    | be set by the first process to open an environment, as it affects the size
    | of the lock file and shared memory area. Attempts to simultaneously
    | start more than this many read transactions will fail.
    | max_dbs: Maximum number of databases available. If 0, assume environment
    | will be used as a single database.

    :param queue_dir: where the queue is stored
    :param max_reader_num: maximal number of readers
    :param max_db_num: maximal number of databases
    :param max_db_size_bytes: maximal size of database (in bytes)
    :return: Load or if needed create LMDB from directory
    """
    if not queue_dir.exists():
        raise RuntimeError(
            "The queue directory does not exist: {}".format(queue_dir))

    env = lmdb.open(
        path=queue_dir.as_posix(),
        map_size=max_db_size_bytes,
        subdir=True,
        max_readers=max_reader_num,
        max_dbs=max_db_num)
    return env


def _prune_dangling_messages_for(queue: '_Queue',
                                 subscriber_ids: Set[str]) -> None:
    """
    Prune all dangling messages for subscribers of a queue from LMDB.

    :param queue: of which dangling messages should be pruned
    :param subscriber_ids: subscribers of which dangling msgs should be pruned
    """
    assert queue.env is not None
    with queue.env.begin(write=True) as txn:
        pending_db = queue.env.open_db(
            key=persipubsub.database.PENDING_DB, txn=txn, create=False)
        meta_db = queue.env.open_db(
            key=persipubsub.database.META_DB, txn=txn, create=False)
        data_db = queue.env.open_db(
            key=persipubsub.database.DATA_DB, txn=txn, create=False)

    # Definition of dangling messages:
    #   - having no pending subscribers
    #   - exists longer than timeout allows

    msgs_to_delete = set()  # type: Set[bytes]
    with queue.env.begin(db=pending_db) as txn:
        cursor = txn.cursor()
        cursor.first()
        for key, pending_subscribers_num in cursor:
            if persipubsub.database.bytes_to_int(pending_subscribers_num) == 0:
                msgs_to_delete.add(key)

    # Some subscriber might still await these messages in their own queue after
    # the high water mark timeout of the database.
    # That means that these messages also need to be removed from all
    # subscribers queues.
    msgs_to_delete_timeout = set()  # type: Set[bytes]
    assert queue.hwm is not None
    with queue.env.begin(db=meta_db) as txn:
        cursor = txn.cursor()

        timestamp_now = datetime.datetime.utcnow().timestamp()
        for key, timestamp in cursor:
            if int(timestamp_now) - persipubsub.database.bytes_to_int(
                    timestamp) > queue.hwm.message_timeout:
                msgs_to_delete_timeout.add(key)

    msgs_to_delete = msgs_to_delete.union(msgs_to_delete_timeout)
    with queue.env.begin(write=True) as txn:
        for key in msgs_to_delete:
            txn.delete(key=key, db=pending_db)
            txn.delete(key=key, db=meta_db)
            txn.delete(key=key, db=data_db)

        for sub_id in subscriber_ids:
            sub_db = queue.env.open_db(
                key=persipubsub.database.str_to_bytes(sub_id),
                txn=txn,
                create=False)
            for key in msgs_to_delete_timeout:
                txn.delete(key=key, db=sub_db)


class _Queue:
    """
    Represent a message queue.

    The messages come from many publishers and many subscribers.
    They are stored persistently in an LMDB database.

    :ivar path: to the queue
    :vartype config_pth: pathlib.Path
    :ivar env: LMDB environment on disk
    :vartype env: lmdb.Environment
    :ivar hwm: High water mark limit for queue
    :vartype hwm: HighWaterMark
    :ivar strategy:
        Strategy which will be used to remove messages when high water mark is
        reached.
    :vartype strategy: Strategy
    :ivar subscriber_ids: all subscribers of the queue
    :vartype subscriber_ids: Set[str]
    """

    def __init__(self) -> None:
        """Initialize class object."""
        self.path = None  # type: Optional[pathlib.Path]
        self.env = None  # type: Optional[lmdb.Environment]
        self.hwm = None  # type: Optional[HighWaterMark]
        self.strategy = None  # type: Optional[Strategy]
        self.subscriber_ids = None  # type: Optional[Set[str]]
        self.closed = False

    def __enter__(self) -> '_Queue':
        """Enter the context and give the queue prepared in the constructor."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Leave the context."""
        self.close()

    def close(self) -> None:
        """Close queue."""
        self.closed = True

    @icontract.require(lambda self: not self.closed)
    def init(self,
             path: Union[pathlib.Path, str],
             env: Optional[lmdb.Environment] = None) -> None:
        """
        Initialize the queue.

        :param path: where the queue is stored
        :param env: open LMDB environment
        """
        # pylint: disable=too-many-arguments
        # pylint: disable=too-many-locals
        self.path = path if isinstance(path, pathlib.Path) \
            else pathlib.Path(path)

        if env is not None:
            self.env = env
        else:
            self.env = _initialize_environment(
                queue_dir=self.path,
                max_reader_num=persipubsub.database.DEFAULT_MAX_READERS,
                max_db_num=persipubsub.database.DEFAULT_MAX_DB_NUM,
                max_db_size_bytes=persipubsub.database.DEFAULT_MAX_DB_SIZE)

        with self.env.begin(write=True) as txn:
            _ = self.env.open_db(
                key=persipubsub.database.DATA_DB, txn=txn, create=True)
            _ = self.env.open_db(
                key=persipubsub.database.PENDING_DB, txn=txn, create=True)
            _ = self.env.open_db(
                key=persipubsub.database.META_DB, txn=txn, create=True)
            _ = self.env.open_db(
                key=persipubsub.database.QUEUE_DB, txn=txn, create=True)
            _ = self.env.open_db(
                key=persipubsub.database.SUBSCRIBER_DB, txn=txn, create=True)

        queue_data = persipubsub.database.retrieve_queue_data(env=self.env)

        self.hwm = HighWaterMark(
            message_timeout=queue_data.message_timeout,
            max_messages=queue_data.max_messages,
            hwm_db_size=queue_data.hwm_db_size)

        self.strategy = _parse_strategy(identifier=queue_data.strategy)

        self.subscriber_ids = queue_data.subscriber_ids

    @icontract.require(lambda self: not self.closed)
    def put(self, msg: bytes) -> None:
        """
        Put message to LMDB in one transaction.

        :param msg: message in bytes
        :return:
        """
        # every publisher always prunes queue before sending a message.
        self.cleanup()
        msg_id = str(datetime.datetime.utcnow().timestamp()) + str(uuid.uuid4())
        assert self.env is not None
        assert self.subscriber_ids is not None
        with self.env.begin(write=True) as txn:
            pending_db = self.env.open_db(
                key=persipubsub.database.PENDING_DB, txn=txn, create=False)
            txn.put(
                key=persipubsub.database.str_to_bytes(msg_id),
                value=persipubsub.database.int_to_bytes(
                    len(self.subscriber_ids)),
                db=pending_db)

            meta_db = self.env.open_db(
                key=persipubsub.database.META_DB, txn=txn, create=False)
            txn.put(
                key=persipubsub.database.str_to_bytes(msg_id),
                value=persipubsub.database.int_to_bytes(
                    int(datetime.datetime.utcnow().timestamp())),
                db=meta_db)

            data_db = self.env.open_db(
                key=persipubsub.database.DATA_DB, txn=txn, create=False)
            txn.put(
                key=persipubsub.database.str_to_bytes(msg_id),
                value=msg,
                db=data_db)

            for sub in self.subscriber_ids:
                sub_db = self.env.open_db(
                    key=persipubsub.database.str_to_bytes(sub),
                    txn=txn,
                    create=False)
                txn.put(
                    key=persipubsub.database.str_to_bytes(msg_id), db=sub_db)

    @icontract.require(lambda self: not self.closed)
    def put_many_flush_once(self, msgs: List[bytes]) -> None:
        """
        Put multiple message to LMDB in one transaction.

        :param msgs: messages in bytes
        :return:
        """
        # every publisher always prunes queue before sending a message.
        self.cleanup()
        assert self.env is not None
        assert self.subscriber_ids is not None
        with self.env.begin(write=True) as txn:

            pending_db = self.env.open_db(
                key=persipubsub.database.PENDING_DB, txn=txn, create=False)

            meta_db = self.env.open_db(
                key=persipubsub.database.META_DB, txn=txn, create=False)

            data_db = self.env.open_db(
                key=persipubsub.database.DATA_DB, txn=txn, create=False)

            sub_dbs = set()  # type: Set[lmdb.Environment]
            for sub in self.subscriber_ids:
                sub_dbs.add(
                    self.env.open_db(
                        key=persipubsub.database.str_to_bytes(sub),
                        txn=txn,
                        create=False))

            for msg in msgs:
                msg_id = str(datetime.datetime.utcnow().timestamp()) + str(
                    uuid.uuid4())

                txn.put(
                    key=persipubsub.database.str_to_bytes(msg_id),
                    value=persipubsub.database.int_to_bytes(
                        len(self.subscriber_ids)),
                    db=pending_db)

                txn.put(
                    key=persipubsub.database.str_to_bytes(msg_id),
                    value=persipubsub.database.int_to_bytes(
                        int(datetime.datetime.utcnow().timestamp())),
                    db=meta_db)

                txn.put(
                    key=persipubsub.database.str_to_bytes(msg_id),
                    value=msg,
                    db=data_db)

                for sub_db in sub_dbs:
                    txn.put(
                        key=persipubsub.database.str_to_bytes(msg_id),
                        db=sub_db)

    @icontract.require(lambda self: not self.closed)
    def front(self, sub_id: str) -> Tuple[Optional[bytes], Optional[bytes]]:
        """
        Peek at next message in LMDB.

        Load from LMDB into memory and process msg afterwards.

        :param sub_id: Subscriber ID
        :return:
        """
        assert self.env is not None
        with self.env.begin(write=False) as txn:
            sub_db = self.env.open_db(
                key=persipubsub.database.str_to_bytes(sub_id),
                txn=txn,
                create=False)
            data_db = self.env.open_db(
                key=persipubsub.database.DATA_DB, txn=txn, create=False)

            cursor = txn.cursor(db=sub_db)
            # check if database is not empty
            if cursor.first():
                key = cursor.key()
                msg = txn.get(key=key, db=data_db)
            else:
                key = None
                msg = None

        return key, msg

    @icontract.require(lambda self: not self.closed)
    def pop(self, sub_id: str, msg_id: Optional[bytes] = None) -> None:
        """
        Remove msg from the subscriber's queue and reduce pending subscribers.

        :param sub_id: Subscriber ID
        :return:
        """
        assert self.env is not None
        with self.env.begin(write=True) as txn:
            sub_db = self.env.open_db(
                key=persipubsub.database.str_to_bytes(sub_id),
                txn=txn,
                create=False)
            pending_db = self.env.open_db(
                key=persipubsub.database.PENDING_DB, txn=txn, create=False)

            cursor = txn.cursor(db=sub_db)
            # check if database is not empty
            if cursor.first():
                if msg_id is None:
                    key = cursor.key()
                else:
                    key = msg_id

                cursor.pop(key=key)
                pending_value = txn.get(key=key, db=pending_db)
                pending_num = persipubsub.database.bytes_to_int(pending_value)
                decreased_pending_num = pending_num - 1
                assert decreased_pending_num >= 0
                txn.put(
                    key=key,
                    value=persipubsub.database.int_to_bytes(
                        decreased_pending_num),
                    db=pending_db)
            else:
                raise RuntimeError("No message to pop")

    @icontract.require(lambda self: not self.closed)
    def prune_dangling_messages(self) -> None:
        """
        Prune dangling messages in the queue.

        Definition of dangling messages:
        - having no pending subscribers
        - exists longer than timeout allows
        :return:
        """
        assert self.subscriber_ids is not None
        _prune_dangling_messages_for(
            queue=self, subscriber_ids=self.subscriber_ids)

    @icontract.require(lambda self: not self.closed)
    def check_current_lmdb_size(self) -> int:
        """
        Check current LMDB size in bytes.

        Check size of data database by approximating size with multiplying page
        size with number of pages.

        :return: data database size in bytes
        """
        lmdb_size_bytes = 0
        assert self.env is not None
        with self.env.begin(write=False) as txn:
            data_db = self.env.open_db(
                key=persipubsub.database.DATA_DB, txn=txn, create=False)
            data_stat = txn.stat(db=data_db)
            lmdb_size_bytes += data_stat['psize'] * (
                data_stat['branch_pages'] + data_stat['leaf_pages'] +
                data_stat['overflow_pages'])

        return lmdb_size_bytes

    @icontract.require(lambda self: not self.closed)
    def count_msgs(self) -> int:
        """
        Count number of messages stored in named database 'meta_db'.

        :return: number of messages in database
        """
        assert self.env is not None
        with self.env.begin(write=False) as txn:
            meta_db = self.env.open_db(
                key=persipubsub.database.META_DB, txn=txn, create=False)
            meta_stat = txn.stat(db=meta_db)  # type: Dict[str, int]

        return meta_stat['entries']

    @icontract.require(lambda self: not self.closed)
    def cleanup(self) -> None:
        """
        Clean database when needed.

        There are two steps of cleaning. The soft prune which just removes the
        dangling messages. If after this step the queue is still over the
        high water mark, the hard prune is necessary. It will delete one half of
        the messages depending on the pruning strategy.

        :return:
        """
        assert self.hwm is not None
        self.prune_dangling_messages()
        msgs_num = self.count_msgs()
        if msgs_num >= self.hwm.max_messages:
            self.prune_half_of_messages()
        lmdb_size_bytes = self.check_current_lmdb_size()
        if lmdb_size_bytes >= self.hwm.hwm_lmdb_size:
            self.prune_half_of_messages()

    @icontract.require(lambda self: not self.closed)
    def prune_half_of_messages(self) -> None:
        """
        Prune one half of the messages stored.

        Depending on the strategy the first or the last half will be deleted.

        :return:
        """
        # pylint: disable=too-many-locals
        messages_to_delete = set()  # type: Set[bytes]
        assert self.env is not None
        with self.env.begin(write=False) as txn:
            meta_db = self.env.open_db(
                key=persipubsub.database.META_DB, txn=txn, create=False)
            meta_stat = txn.stat(db=meta_db)
            entries = meta_stat['entries']

            cursor = txn.cursor(db=meta_db)
            if self.strategy == Strategy.PRUNE_FIRST:

                cursor.first()
                for index, key in enumerate(
                        cursor.iternext(keys=True, values=False)):
                    messages_to_delete.add(key)
                    if index >= int(entries / 2):
                        break

            elif self.strategy == Strategy.PRUNE_LAST:
                cursor.last()
                for index, key in enumerate(
                        cursor.iterprev(keys=True, values=False)):
                    messages_to_delete.add(key)
                    if index >= int(entries / 2):
                        break
            else:
                raise RuntimeError("Pruning strategy not set.")

        assert self.subscriber_ids is not None
        with self.env.begin(write=True) as txn:
            pending_db = self.env.open_db(
                key=persipubsub.database.PENDING_DB, txn=txn, create=False)
            meta_db = self.env.open_db(
                key=persipubsub.database.META_DB, txn=txn, create=False)
            data_db = self.env.open_db(
                key=persipubsub.database.DATA_DB, txn=txn, create=False)

            dbs = [pending_db, meta_db, data_db]

            for sub in self.subscriber_ids:
                sub_db = self.env.open_db(
                    key=persipubsub.database.str_to_bytes(sub),
                    txn=txn,
                    create=False)
                dbs.append(sub_db)

            for key in messages_to_delete:
                for db in dbs:  # pylint: disable=invalid-name
                    txn.delete(key=key, db=db)
