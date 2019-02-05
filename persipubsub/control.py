#!/usr/bin/env python3
"""Setup and control persistent distribution of messages."""

import pathlib
from typing import List, Optional, Sequence, Set

import icontract
import lmdb  # pylint: disable=unused-import

import persipubsub.queue

# pylint: disable=protected-access


def set_queue_parameters(max_reader_num: int, max_db_num: int,
                         max_db_size_bytes: int, env: lmdb.Environment) -> None:
    """
    Store queue parameter in lmdb.

    :param max_reader_num:  max number of reader of the lmdb
    :param max_db_num: max number of named databases
    :param max_db_size_bytes: max size of the lmdb in bytes
    :return:
    """
    with env.begin(write=True) as txn:
        queue_db = env.open_db(persipubsub.QUEUE_DB, txn=txn, create=True)
        txn.put(
            key=persipubsub.MAX_READER_NUM_KEY,
            value=persipubsub.int_to_bytes(max_reader_num),
            db=queue_db)
        txn.put(
            key=persipubsub.MAX_DB_NUM_KEY,
            value=persipubsub.int_to_bytes(max_db_num),
            db=queue_db)
        txn.put(
            key=persipubsub.MAX_DB_SIZE_BYTES_KEY,
            value=persipubsub.int_to_bytes(max_db_size_bytes),
            db=queue_db)


def set_hwm(hwm: persipubsub.queue._HighWaterMark,
            env: lmdb.Environment) -> None:
    """
    Set high water mark values for queue.

    :param hwm: high water mark values
    :return:
    """
    with env.begin(write=True) as txn:
        queue_db = env.open_db(persipubsub.QUEUE_DB, txn=txn, create=True)
        txn.put(
            key=persipubsub.HWM_DB_SIZE_BYTES_KEY,
            value=persipubsub.int_to_bytes(hwm.hwm_lmdb_size_bytes),
            db=queue_db)
        txn.put(
            key=persipubsub.MAX_MSGS_NUM_KEY,
            value=persipubsub.int_to_bytes(hwm.max_msgs_num),
            db=queue_db)
        txn.put(
            key=persipubsub.MSG_TIMEOUT_SECS_KEY,
            value=persipubsub.int_to_bytes(hwm.msg_timeout_secs),
            db=queue_db)


def set_strategy(strategy: persipubsub.queue._Strategy,
                 env: lmdb.Environment) -> None:
    """
    Set pruning strategy for queue.

    :param strategy: pruning strategy
    :return:
    """
    with env.begin(write=True) as txn:
        queue_db = env.open_db(persipubsub.QUEUE_DB, txn=txn, create=True)
        txn.put(
            key=persipubsub.STRATEGY_KEY,
            value=persipubsub.encoding(str(strategy.name)),
            db=queue_db)


@icontract.require(lambda sub_id: sub_id.find(' ') == -1)
def _add_sub(sub_id: str, env: lmdb.Environment) -> None:
    """
    Add a subscriber and create its lmdb.

    :param sub_id: ID of the subscriber which should be added
    """
    with env.begin(write=True) as txn:
        _ = env.open_db(key=persipubsub.encoding(sub_id), txn=txn, create=True)

        queue_db = env.open_db(persipubsub.QUEUE_DB, txn=txn, create=False)
        subscriber_ids = txn.get(
            key=persipubsub.SUBSCRIBER_IDS_KEY, db=queue_db)

        if subscriber_ids is None:
            subscriber_list = []  # type: List[str]
        else:
            subscriber_list = subscriber_ids.decode(
                persipubsub.ENCODING).split(' ')
        subscriber_set = set(subscriber_list)
        subscriber_set.add(sub_id)
        subscriber_str = " ".join(subscriber_set)
        txn.put(
            key=persipubsub.SUBSCRIBER_IDS_KEY,
            value=subscriber_str.encode(persipubsub.ENCODING),
            db=queue_db)


class Control:
    """Control and maintain one queue."""

    def __init__(self, path: pathlib.Path):
        """
        Initialize control class.

        :param path: to the queue.
        """
        self.path = path
        self.queue = None  # type: Optional[persipubsub.queue._Queue]
        self.subscriber_ids = set()  # type: Set[str]

    # pylint: disable=too-many-arguments
    def init(self,
             subscriber_ids: Optional[Sequence[str]] = None,
             max_readers: int = 1024,
             max_size: int = 32 * 1024**3,
             high_watermark: persipubsub.queue._HighWaterMark = persipubsub.
             queue._HighWaterMark(),
             strategy: persipubsub.queue._Strategy = persipubsub.queue.
             _Strategy.prune_first) -> None:
        """
        Initialize control with a (re)initialized queue.

        :param subscriber_ids: subscribers of the queue
        :param max_readers: max number of reader of the lmdb
        :param max_size: max size of the lmdb in bytes
        :param high_watermark: high water mark limit of the queue
        :return:
        """
        if self.check_queue_is_initialized():
            self.reinitialize_queue()
        else:
            if subscriber_ids is None:
                subscriber_ids = []
            assert isinstance(subscriber_ids, Sequence)
            self.initialize_queue(
                subscriber_ids=subscriber_ids,
                max_readers=max_readers,
                max_size=max_size,
                high_watermark=high_watermark,
                strategy=strategy)

    def reinitialize_queue(self) -> None:
        """Reinitialize the queue which is maintained by the control."""
        self.queue = persipubsub.queue._Queue()
        self.queue.init(path=self.path)
        assert isinstance(self.queue.sub_list, List)
        self.subscriber_ids = set(self.queue.sub_list)

    # pylint: disable=too-many-arguments
    def initialize_queue(self,
                         subscriber_ids: Sequence[str],
                         max_readers: int = 1024,
                         max_size: int = 32 * 1024**3,
                         high_watermark: persipubsub.queue.
                         _HighWaterMark = persipubsub.queue._HighWaterMark(),
                         strategy: persipubsub.queue._Strategy = persipubsub.
                         queue._Strategy.prune_first) -> None:
        """
        Initialize queue.

        :param subscriber_ids: subscribers of the queue
        :param max_readers: max number of reader of the lmdb
        :param max_size: max size of the lmdb in bytes
        :param high_watermark: high water mark limit of the queue
        :return:
        """
        if not self.path.exists():
            self.path.mkdir(parents=True, exist_ok=True)

        self.subscriber_ids = set(subscriber_ids)
        # Databases needed for queue:
        # 5 queues (main db, data db, meta db, pending db, queue db)
        # + each subscriber has its own db
        # TODO(snaji): should it be max(len(sub_ids), max_readers)?
        max_db_num = 5 + len(self.subscriber_ids)

        env = persipubsub.queue._initialize_environment(
            queue_dir=self.path,
            max_db_num=max_db_num,
            max_reader_num=max_readers,
            max_db_size_bytes=max_size)

        set_queue_parameters(
            max_reader_num=max_readers,
            max_db_num=max_db_num,
            max_db_size_bytes=max_size,
            env=env)

        set_hwm(hwm=high_watermark, env=env)
        set_strategy(strategy=strategy, env=env)

        for sub in self.subscriber_ids:
            _add_sub(sub_id=sub, env=env)

        # load initialized queue
        self.queue = persipubsub.queue._Queue()
        self.queue.init(path=self.path)

    def check_queue_is_initialized(self) -> bool:
        """
        Check if queue is initialized.

        :return is initialized when all values for the given keys are set
        """
        keys = [
            persipubsub.MAX_DB_SIZE_BYTES_KEY, persipubsub.MAX_DB_NUM_KEY,
            persipubsub.MSG_TIMEOUT_SECS_KEY, persipubsub.MAX_MSGS_NUM_KEY,
            persipubsub.HWM_DB_SIZE_BYTES_KEY, persipubsub.STRATEGY_KEY,
            persipubsub.SUBSCRIBER_IDS_KEY, persipubsub.MAX_READER_NUM_KEY
        ]  # type: List[bytes]

        for key in keys:

            try:
                value = persipubsub.get_queue_data(path=self.path, key=key)
            except lmdb.NotFoundError:
                return False

            if value is None:
                return False

        return True

    def clear_all_subscribers(self) -> None:
        """Clear all subscriber and delete all messages for queue."""
        assert isinstance(self.queue, persipubsub.queue._Queue)
        assert isinstance(self.queue.env, lmdb.Environment)
        with self.queue.env.begin(write=True) as txn:
            for sub_id in self.subscriber_ids:
                sub_db = self.queue.env.open_db(
                    key=persipubsub.encoding(sub_id), txn=txn, create=False)
                txn.drop(db=sub_db)

                pending_db = self.queue.env.open_db(
                    key=persipubsub.PENDING_DB, txn=txn, create=False)
                txn.drop(db=pending_db, delete=False)
                meta_db = self.queue.env.open_db(
                    key=persipubsub.META_DB, txn=txn, create=False)
                txn.drop(db=meta_db, delete=False)
                data_db = self.queue.env.open_db(
                    key=persipubsub.DATA_DB, txn=txn, create=False)
                txn.drop(db=data_db, delete=False)

    def prune_dangling_messages(self) -> None:
        """Prune all dangling messages from the lmdb."""
        assert isinstance(self.queue, persipubsub.queue._Queue)
        persipubsub.queue._prune_dangling_messages_for(
            queue=self.queue, sub_list=list(self.subscriber_ids))

    # pylint: disable=too-many-locals
    def _prune_all_messages_for(self, sub_id: str) -> None:
        """
        Prune all messages of a subscriber.

        :param sub_id: ID of the subscriber of which all messages should be
            pruned
        :param queue: of which some subscriber's messages should be pruned
        """
        msg_of_sub = set()
        assert isinstance(self.queue, persipubsub.queue._Queue)
        assert isinstance(self.queue.env, lmdb.Environment)
        with self.queue.env.begin(write=True) as txn:
            sub_db = self.queue.env.open_db(
                key=persipubsub.encoding(sub_id), txn=txn, create=False)

            cursor = txn.cursor(db=sub_db)
            # check if database is not empty
            if cursor.first():
                for key in cursor.iternext(keys=True, values=False):
                    msg_of_sub.add(key)

            txn.drop(db=sub_db, delete=False)

            pending_db = self.queue.env.open_db(
                key=persipubsub.PENDING_DB, txn=txn, create=False)

            for key in msg_of_sub:
                pending_value = txn.get(key=key, db=pending_db)
                pending_num = persipubsub.bytes_to_int(pending_value)
                decreased_pending_num = pending_num - 1
                assert decreased_pending_num >= 0
                txn.put(
                    key=key,
                    value=persipubsub.int_to_bytes(decreased_pending_num),
                    db=pending_db)

    def _remove_sub(self, sub_id: str, env: lmdb.Environment) -> None:
        """
        Remove a subscriber and delete all its messages.

        :param sub_id: ID of the subscriber which should be removed
        :param queue: from which the subscriber is removed
        """
        msg_of_sub = set()
        with env.begin(write=True) as txn:
            sub_db = env.open_db(
                key=persipubsub.encoding(sub_id), txn=txn, create=False)
            cursor = txn.cursor(db=sub_db)
            # check if database is not empty
            if cursor.first():
                for key in cursor.iternext(keys=True, values=False):
                    msg_of_sub.add(key)
            txn.drop(db=sub_db)

            pending_db = env.open_db(
                key=persipubsub.PENDING_DB, txn=txn, create=False)

            for key in msg_of_sub:
                pending_value = txn.get(key=key, db=pending_db)
                pending_num = persipubsub.bytes_to_int(pending_value)
                decreased_pending_num = pending_num - 1
                assert decreased_pending_num >= 0
                txn.put(
                    key=key,
                    value=persipubsub.int_to_bytes(decreased_pending_num),
                    db=pending_db)

            queue_db = env.open_db(persipubsub.QUEUE_DB, txn=txn, create=False)
            subscriber_ids = txn.get(
                key=persipubsub.SUBSCRIBER_IDS_KEY, db=queue_db)

            subscriber_list = subscriber_ids.decode(
                persipubsub.ENCODING).split(' ')
            subscriber_set = set(subscriber_list)
            subscriber_set.remove(sub_id)
            subscriber_str = " ".join(subscriber_set)
            txn.put(
                key=persipubsub.SUBSCRIBER_IDS_KEY,
                value=subscriber_str.encode(persipubsub.ENCODING),
                db=queue_db)

            self.subscriber_ids.remove(sub_id)
