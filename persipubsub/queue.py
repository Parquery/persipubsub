#!/usr/bin/env python3
"""Store messages in a local LMDB."""
import datetime
import enum
import pathlib
import uuid
from typing import Any, Dict, List, Optional, Union

import lmdb

import persipubsub

MSG_TIMEOUT_SECS = 500  # type: int
MAX_MSGS_NUM = 1024 * 64
HWM_LMDB_SIZE_BYTES = 30 * 1024**3  # type: int


class Strategy(enum.Enum):
    """Hold possible strategies."""

    prune_first = 0
    prune_last = 1


def _parse_strategy(strategy: str) -> Strategy:
    """
    Parse overflow strategy.

    :param strategy: Strategy stored in LMDB
    :return: set overflow strategy
    """
    if Strategy.prune_first.name == strategy:
        return Strategy.prune_first

    if Strategy.prune_last.name == strategy:
        return Strategy.prune_last

    raise ValueError("Unknown strategy: '{}' choosen.".format(strategy))


class HighWaterMark:
    """Hold high water mark limits."""

    def __init__(self,
                 msg_timeout_secs: Optional[int] = None,
                 max_msgs_num: Optional[int] = None,
                 hwm_lmdb_size_bytes: Optional[int] = None) -> None:
        """
        Initialize.

        :param msg_timeout_secs: time after which msg is classified as dangling
        msg (secs)
        :param max_msgs_num: maximal amount of msg
        :param hwm_lmdb_size_bytes: high water mark for total size of LMDB
        (bytes)
        """
        self.msg_timeout_secs = MSG_TIMEOUT_SECS \
            if msg_timeout_secs is None else msg_timeout_secs  # type: int
        self.max_msgs_num = MAX_MSGS_NUM \
            if max_msgs_num is None else max_msgs_num  # type: int
        self.hwm_lmdb_size_bytes = HWM_LMDB_SIZE_BYTES \
            if hwm_lmdb_size_bytes is None else hwm_lmdb_size_bytes  # type: int


def _initialize_environment(
        queue_dir: pathlib.Path,
        max_reader_num: int = 1024,
        max_db_num: int = 1024,
        max_db_size_bytes: int = 32 * 1024**3) -> lmdb.Environment:
    """
    Initialize the queue; the queue directory is assumed to exist.

    :param queue_dir: where the queue is stored
    :param max_reader_num: maximal number of readers
    :param max_db_num: maximal number of databases
    :param max_db_size_bytes: maximal size of database (bytes)
    :return: Load or if needed create LMDB from directory
    """
    if not queue_dir.exists():
        raise RuntimeError(
            "The queue directory does not exist: {}".format(queue_dir))

    # max_spare_txn: Read-only transactions to cache after becoming unused.
    # Caching transactions avoids two allocations, one lock and linear scan of
    # the shared environment per invocation of begin(), Transaction, get(),
    # gets(), or cursor(). Should match the process’s maximum expected
    # concurrent transactions (e.g. thread count).
    # Setting max_spare_txn equals 0 doesn't allow any caching for read-only
    # transactions to avoid the error:
    # MDB_BAD_RSLOT: Invalid reuse of reader locktable slot.
    # Error only occures when one publisher/subscriber is used in multiple
    # processes. So max_spare_txn equal 1 is fine as we expect a publisher and
    # subscriber running on the same process.
    env = lmdb.open(
        path=queue_dir.as_posix(),
        map_size=max_db_size_bytes,
        subdir=True,
        max_readers=max_reader_num,
        max_dbs=max_db_num,
        max_spare_txns=1)
    return env


def _prune_dangling_messages_for(queue: '_Queue',
                                 subscriber_ids: List[str]) -> None:
    """
    Prune all dangling messages for subscribers of a queue from LMDB.

    :param queue: of which dangling messages should be pruned
    :param subscriber_ids: subscribers of which dangling msgs should be pruned
    """
    assert isinstance(queue.env, lmdb.Environment)
    with queue.env.begin(write=True) as txn:
        pending_db = queue.env.open_db(
            key=persipubsub.PENDING_DB, txn=txn, create=False)
        meta_db = queue.env.open_db(
            key=persipubsub.META_DB, txn=txn, create=False)
        data_db = queue.env.open_db(
            key=persipubsub.DATA_DB, txn=txn, create=False)

    # Definition of dangling messages:
    #   - having no pending subscribers
    #   - exists longer than timeout allows
    msgs_to_delete = set()
    with queue.env.begin(db=pending_db) as txn:
        cursor = txn.cursor()
        cursor.first()
        for key, pending_subscribers_num in cursor:
            if persipubsub.bytes_to_int(pending_subscribers_num) == 0:
                msgs_to_delete.add(key)

    # subscriber might still await these messages after the timeout.
    # These messages needs also to be removed from all subscribers.
    msgs_to_delete_timeout = set()
    assert isinstance(queue.hwm, HighWaterMark)
    with queue.env.begin(db=meta_db) as txn:
        cursor = txn.cursor()

        timestamp_now = datetime.datetime.utcnow().timestamp()
        for key, timestamp in cursor:
            if int(timestamp_now) - persipubsub.bytes_to_int(timestamp) \
                    > queue.hwm.msg_timeout_secs:
                msgs_to_delete_timeout.add(key)

    msgs_to_delete = msgs_to_delete.union(msgs_to_delete_timeout)
    with queue.env.begin(write=True) as txn:
        for key in msgs_to_delete:
            txn.delete(key=key, db=pending_db)
            txn.delete(key=key, db=meta_db)
            txn.delete(key=key, db=data_db)

    with queue.env.begin(write=True) as txn:
        for sub_id in subscriber_ids:
            sub_db = queue.env.open_db(
                key=persipubsub.str_to_bytes(sub_id), txn=txn, create=False)
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
    :vartype sub_list: List[str]
    """

    def __init__(self) -> None:
        """Initialize class object."""
        self.path = None  # type: Optional[pathlib.Path]
        self.env = None  # type: Optional[lmdb.Environment]
        self.hwm = None  # type: Optional[HighWaterMark]
        self.strategy = None  # type: Optional[Strategy]
        self.subscriber_ids = None  # type: Optional[List[str]]

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

        if isinstance(env, lmdb.Environment):
            self.env = env
        else:
            self.env = _initialize_environment(
                queue_dir=self.path,
                max_reader_num=persipubsub.MAX_READER_NUM,
                max_db_num=persipubsub.MAX_DB_NUM,
                max_db_size_bytes=persipubsub.MAX_DB_SIZE_BYTES)

        with self.env.begin(write=True) as txn:
            _ = self.env.open_db(key=persipubsub.DATA_DB, txn=txn, create=True)
            _ = self.env.open_db(
                key=persipubsub.PENDING_DB, txn=txn, create=True)
            _ = self.env.open_db(key=persipubsub.META_DB, txn=txn, create=True)
            _ = self.env.open_db(key=persipubsub.QUEUE_DB, txn=txn, create=True)

        msg_timeout_secs_bytes = persipubsub.lookup_queue_data(
            key=persipubsub.MSG_TIMEOUT_SECS_KEY, env=self.env)
        msg_timeout_secs = msg_timeout_secs_bytes \
            if msg_timeout_secs_bytes is None else persipubsub.bytes_to_int(
            msg_timeout_secs_bytes)  # type: Optional[int]

        max_msgs_num_bytes = persipubsub.lookup_queue_data(
            key=persipubsub.MAX_MSGS_NUM_KEY, env=self.env)
        max_msgs_num = max_msgs_num_bytes \
            if max_msgs_num_bytes is None else persipubsub.bytes_to_int(
            max_msgs_num_bytes)  # type: Optional[int]

        hwm_lmdb_size_bytes = persipubsub.lookup_queue_data(
            key=persipubsub.HWM_DB_SIZE_BYTES_KEY, env=self.env)
        hwm_lmdb_size = hwm_lmdb_size_bytes \
            if hwm_lmdb_size_bytes is None else persipubsub.bytes_to_int(
            hwm_lmdb_size_bytes)  # type: Optional[int]

        self.hwm = HighWaterMark(
            msg_timeout_secs=msg_timeout_secs,
            max_msgs_num=max_msgs_num,
            hwm_lmdb_size_bytes=hwm_lmdb_size)

        strategy = persipubsub.lookup_queue_data(
            key=persipubsub.STRATEGY_KEY, env=self.env)

        strategy_decoded = "" if strategy is None else persipubsub.bytes_to_str(
            encoded_str=strategy)

        self.strategy = _parse_strategy(strategy=strategy_decoded)

        subscriber_list = persipubsub.lookup_queue_data(
            key=persipubsub.SUBSCRIBER_IDS_KEY, env=self.env)

        if subscriber_list is None:
            self.subscriber_ids = []
        else:
            self.subscriber_ids = persipubsub.bytes_to_str(
                encoded_str=subscriber_list).split(' ')

    def __enter__(self) -> '_Queue':
        """Enter the context and give the queue prepared in the constructor."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Leave the context."""

    def put(self, msg: bytes) -> None:
        """
        Put message to LMDB in one transaction.

        :param msg: message in bytes
        :return:
        """
        # every publisher always prunes queue before sending a message.
        self.vacuum()
        msg_id = str(datetime.datetime.utcnow().timestamp()) + str(uuid.uuid4())
        assert isinstance(self.env, lmdb.Environment)
        assert isinstance(self.subscriber_ids, List)
        with self.env.begin(write=True) as txn:
            pending_db = self.env.open_db(
                key=persipubsub.PENDING_DB, txn=txn, create=False)
            txn.put(
                key=persipubsub.str_to_bytes(msg_id),
                value=persipubsub.int_to_bytes(len(self.subscriber_ids)),
                db=pending_db)

            meta_db = self.env.open_db(
                key=persipubsub.META_DB, txn=txn, create=False)
            txn.put(
                key=persipubsub.str_to_bytes(msg_id),
                value=persipubsub.int_to_bytes(
                    int(datetime.datetime.utcnow().timestamp())),
                db=meta_db)

            data_db = self.env.open_db(
                key=persipubsub.DATA_DB, txn=txn, create=False)
            txn.put(key=persipubsub.str_to_bytes(msg_id), value=msg, db=data_db)

            for sub in self.subscriber_ids:
                sub_db = self.env.open_db(
                    key=persipubsub.str_to_bytes(sub), txn=txn, create=False)
                txn.put(key=persipubsub.str_to_bytes(msg_id), db=sub_db)

    def put_many_flush_once(self, msgs: List[bytes]) -> None:
        """
        Put multiple message to LMDB in one transaction.

        :param msgs: messages in bytes
        :return:
        """
        # every publisher always prunes queue before sending a message.
        self.vacuum()
        assert isinstance(self.env, lmdb.Environment)
        assert isinstance(self.subscriber_ids, List)
        with self.env.begin(write=True) as txn:

            pending_db = self.env.open_db(
                key=persipubsub.PENDING_DB, txn=txn, create=False)

            meta_db = self.env.open_db(
                key=persipubsub.META_DB, txn=txn, create=False)

            data_db = self.env.open_db(
                key=persipubsub.DATA_DB, txn=txn, create=False)

            sub_dbs = set()
            for sub in self.subscriber_ids:
                sub_dbs.add(
                    self.env.open_db(
                        key=persipubsub.str_to_bytes(sub),
                        txn=txn,
                        create=False))

            for msg in msgs:
                msg_id = str(datetime.datetime.utcnow().timestamp()) + str(
                    uuid.uuid4())

                txn.put(
                    key=persipubsub.str_to_bytes(msg_id),
                    value=persipubsub.int_to_bytes(len(self.subscriber_ids)),
                    db=pending_db)

                txn.put(
                    key=persipubsub.str_to_bytes(msg_id),
                    value=persipubsub.int_to_bytes(
                        int(datetime.datetime.utcnow().timestamp())),
                    db=meta_db)

                txn.put(
                    key=persipubsub.str_to_bytes(msg_id), value=msg, db=data_db)

                for sub_db in sub_dbs:
                    txn.put(key=persipubsub.str_to_bytes(msg_id), db=sub_db)

    def front(self, identifier: str) -> Optional[bytes]:
        """
        Peek at next message in LMDB.

        Load from LMDB into memory and process msg afterwards.

        :param identifier: Subscriber ID
        :return:
        """
        assert isinstance(self.env, lmdb.Environment)
        with self.env.begin(write=False) as txn:
            sub_db = self.env.open_db(
                key=persipubsub.str_to_bytes(identifier), txn=txn, create=False)
            data_db = self.env.open_db(
                key=persipubsub.DATA_DB, txn=txn, create=False)

            cursor = txn.cursor(db=sub_db)
            # check if database is not empty
            if cursor.first():
                key = cursor.key()
                msg = txn.get(key=key, db=data_db)
            else:
                msg = None

        return msg  # type: ignore

    def pop(self, identifier: str) -> None:
        """
        Remove msg from the subscriber's queue and reduce pending subscribers.

        :param identifier: Subscriber ID
        :return:
        """
        assert isinstance(self.env, lmdb.Environment)
        with self.env.begin(write=True) as txn:
            sub_db = self.env.open_db(
                key=persipubsub.str_to_bytes(identifier), txn=txn, create=False)
            pending_db = self.env.open_db(
                key=persipubsub.PENDING_DB, txn=txn, create=False)

            cursor = txn.cursor(db=sub_db)
            # check if database is not empty
            if cursor.first():
                key = cursor.key()
                cursor.pop(key=key)
                pending_value = txn.get(key=key, db=pending_db)
                pending_num = persipubsub.bytes_to_int(pending_value)
                decreased_pending_num = pending_num - 1
                assert decreased_pending_num >= 0
                txn.put(
                    key=key,
                    value=persipubsub.int_to_bytes(decreased_pending_num),
                    db=pending_db)
            else:
                raise RuntimeError("No message to pop")

    def prune_dangling_messages(self) -> None:
        """
        Prune dangling messages in the queue.

        Definition of dangling messages:
        - having no pending subscribers
        - exists longer than timeout allows
        :return:
        """
        assert isinstance(self.subscriber_ids, List)
        _prune_dangling_messages_for(
            queue=self, subscriber_ids=self.subscriber_ids)

    def check_current_lmdb_size(self) -> int:
        """
        Check current LMDB size in bytes.

        Check size of data database by approximating size with multiplying page
        size with number of pages.

        :return: data database size in bytes
        """
        lmdb_size_bytes = 0
        assert isinstance(self.env, lmdb.Environment)
        with self.env.begin(write=False) as txn:
            data_db = self.env.open_db(
                key=persipubsub.DATA_DB, txn=txn, create=False)
            data_stat = txn.stat(db=data_db)
            lmdb_size_bytes += data_stat['psize'] * (
                data_stat['branch_pages'] + data_stat['leaf_pages'] +
                data_stat['overflow_pages'])

        return lmdb_size_bytes

    def count_msgs(self) -> int:
        """
        Count number of messages in database.

        Count number of messages stored in named database 'meta_db'.

        :return: number of messages in database
        """
        assert isinstance(self.env, lmdb.Environment)
        with self.env.begin(write=False) as txn:
            meta_db = self.env.open_db(
                key=persipubsub.META_DB, txn=txn, create=False)
            meta_stat = txn.stat(db=meta_db)  # type: Dict[str, int]

        return meta_stat['entries']

    def vacuum(self) -> None:
        """
        Clean database when needed.

        :return:
        """
        assert isinstance(self.hwm, HighWaterMark)
        self.prune_dangling_messages()
        msgs_num = self.count_msgs()
        if msgs_num >= self.hwm.max_msgs_num:
            self.prune_messages()
        lmdb_size_bytes = self.check_current_lmdb_size()
        if lmdb_size_bytes >= self.hwm.hwm_lmdb_size_bytes:
            self.prune_messages()

    def prune_messages(self) -> None:
        """
        Prune one half of the messages stored.

        Depending on the strategy the first or the last will be deleted.

        :return:
        """
        # pylint: disable=too-many-locals
        messages_to_delete = set()
        assert isinstance(self.env, lmdb.Environment)
        with self.env.begin(write=False) as txn:
            meta_db = self.env.open_db(
                key=persipubsub.META_DB, txn=txn, create=False)
            meta_stat = txn.stat(db=meta_db)
            entries = meta_stat['entries']

            cursor = txn.cursor(db=meta_db)
            if self.strategy == Strategy.prune_first:

                cursor.first()
                for index, key in enumerate(
                        cursor.iternext(keys=True, values=False)):
                    messages_to_delete.add(key)
                    if index >= int(entries / 2):
                        break

            elif self.strategy == Strategy.prune_last:
                cursor.last()
                for index, key in enumerate(
                        cursor.iterprev(keys=True, values=False)):
                    messages_to_delete.add(key)
                    if index >= int(entries / 2):
                        break
            else:
                raise RuntimeError("Pruning strategy not set.")

        assert isinstance(self.subscriber_ids, List)
        with self.env.begin(write=True) as txn:
            pending_db = self.env.open_db(
                key=persipubsub.PENDING_DB, txn=txn, create=False)
            meta_db = self.env.open_db(
                key=persipubsub.META_DB, txn=txn, create=False)
            data_db = self.env.open_db(
                key=persipubsub.DATA_DB, txn=txn, create=False)

            dbs = [pending_db, meta_db, data_db]

            for sub in self.subscriber_ids:
                sub_db = self.env.open_db(
                    key=persipubsub.str_to_bytes(sub), txn=txn, create=False)
                dbs.append(sub_db)

            for key in messages_to_delete:
                for db in dbs:  # pylint: disable=invalid-name
                    txn.delete(key=key, db=db)
