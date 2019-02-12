#!/usr/bin/env python3
"""Setup and control persistent queue."""

import pathlib
from typing import List, Optional, Sequence, Set

import icontract
import lmdb  # pylint: disable=unused-import

import persipubsub.queue

# pylint: disable=protected-access


def set_hwm(hwm: persipubsub.queue.HighWaterMark,
            env: lmdb.Environment) -> None:
    """
    Set high water mark values for queue.

    :param hwm: high water mark values
    :param env: open LMDB environment
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


def set_strategy(strategy: persipubsub.queue.Strategy,
                 env: lmdb.Environment) -> None:
    """
    Set pruning strategy for queue.

    :param strategy: pruning strategy
    :param env: open LMDB environment
    :return:
    """
    with env.begin(write=True) as txn:
        queue_db = env.open_db(persipubsub.QUEUE_DB, txn=txn, create=True)
        txn.put(
            key=persipubsub.STRATEGY_KEY,
            value=persipubsub.str_to_bytes(str(strategy.name)),
            db=queue_db)


@icontract.require(lambda sub_id: ' ' not in sub_id)
def _add_sub(sub_id: str, env: lmdb.Environment) -> None:
    """
    Add a subscriber and create its LMDB.

    :param sub_id: ID of the subscriber which should be added
    :param env: open LMDB environment
    """
    with env.begin(write=True) as txn:
        _ = env.open_db(
            key=persipubsub.str_to_bytes(sub_id), txn=txn, create=True)

        queue_db = env.open_db(persipubsub.QUEUE_DB, txn=txn, create=True)
        subscriber_ids = txn.get(
            key=persipubsub.SUBSCRIBER_IDS_KEY, db=queue_db)

        if subscriber_ids is None:
            subscriber_list = []  # type: List[str]
        else:
            subscriber_list = persipubsub.bytes_to_str(
                encoded_str=subscriber_ids).split(' ')
        subscriber_set = set(subscriber_list)
        subscriber_set.add(sub_id)
        subscriber_str = " ".join(subscriber_set)
        txn.put(
            key=persipubsub.SUBSCRIBER_IDS_KEY,
            value=subscriber_str.encode(persipubsub.ENCODING),
            db=queue_db)


class Control:
    """Control and maintain a queue."""

    def __init__(self,
                 path: pathlib.Path,
                 env: Optional[lmdb.Environment] = None):
        """
        Initialize control class.

        :param path: to the queue.
        :param env: open LMDB environment
        """
        self.path = path
        if not self.path.exists():
            self.path.mkdir(parents=True, exist_ok=True)
        self.queue = None  # type: Optional[persipubsub.queue._Queue]
        self.subscriber_ids = set()  # type: Set[str]

        if isinstance(env, lmdb.Environment):
            self.env = env
        else:
            self.env = persipubsub.queue._initialize_environment(
                queue_dir=self.path,
                max_reader_num=persipubsub.MAX_READER_NUM,
                max_db_num=persipubsub.MAX_DB_NUM,
                max_db_size_bytes=persipubsub.MAX_DB_SIZE_BYTES)

    def init(self,
             subscriber_ids: Optional[Sequence[str]] = None,
             high_watermark: persipubsub.queue.HighWaterMark = persipubsub.
             queue.HighWaterMark(),
             strategy: persipubsub.queue.Strategy = persipubsub.queue.Strategy.
             prune_first) -> None:
        """
        Initialize control with a (re)initialized queue.

        :param subscriber_ids: subscribers of the queue
        :param high_watermark: high water mark limit of the queue
        :param strategy: used to prune queue
        :return:
        """
        # pylint: disable=too-many-arguments
        if self.is_initialized():
            self._reinitialize_queue()
        else:
            if subscriber_ids is None:
                subscriber_ids = []
            assert isinstance(subscriber_ids, Sequence)
            self._initialize_queue(
                subscriber_ids=subscriber_ids,
                high_watermark=high_watermark,
                strategy=strategy)

    def _reinitialize_queue(self) -> None:
        """Reinitialize the queue which is maintained by the control."""
        self.queue = persipubsub.queue._Queue(
        )  # type: persipubsub.queue._Queue
        self.queue.init(path=self.path, env=self.env)
        assert isinstance(self.queue.subscriber_ids, List)
        self.subscriber_ids = set(self.queue.subscriber_ids)

    def _initialize_queue(self,
                          subscriber_ids: Sequence[str],
                          high_watermark: persipubsub.queue.
                          HighWaterMark = persipubsub.queue.HighWaterMark(),
                          strategy: persipubsub.queue.Strategy = persipubsub.
                          queue.Strategy.prune_first) -> None:
        """
        Initialize queue.

        :param subscriber_ids: subscribers of the queue
        :param high_watermark: high water mark limit of the queue
        :param strategy: used to prune queue
        :return:
        """
        self.subscriber_ids = set(subscriber_ids)
        # Databases needed for queue:
        # 4 queues (data db, meta db, pending db, queue db)
        # + each subscriber has its own db

        set_hwm(hwm=high_watermark, env=self.env)
        set_strategy(strategy=strategy, env=self.env)

        for sub in self.subscriber_ids:
            _add_sub(sub_id=sub, env=self.env)

        # load initialized queue
        self.queue = persipubsub.queue._Queue(
        )  # type: persipubsub.queue._Queue
        self.queue.init(path=self.path, env=self.env)

    def is_initialized(self) -> bool:
        """
        Check if queue is initialized.

        :return: is initialized when all values for the given keys are set
        """
        keys = [
            persipubsub.MSG_TIMEOUT_SECS_KEY, persipubsub.MAX_MSGS_NUM_KEY,
            persipubsub.HWM_DB_SIZE_BYTES_KEY, persipubsub.STRATEGY_KEY,
            persipubsub.SUBSCRIBER_IDS_KEY
        ]  # type: List[bytes]

        for key in keys:

            try:
                value = persipubsub.lookup_queue_data(key=key, env=self.env)
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
                    key=persipubsub.str_to_bytes(sub_id), txn=txn, create=False)
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
        """Prune all dangling messages from the LMDB."""
        assert isinstance(self.queue, persipubsub.queue._Queue)
        persipubsub.queue._prune_dangling_messages_for(
            queue=self.queue, subscriber_ids=list(self.subscriber_ids))

    # pylint: disable=too-many-locals
    def _prune_all_messages_for(self, sub_id: str) -> None:
        """
        Prune all messages of a subscriber.

        :param sub_id: ID of the subscriber of which all messages should be
            pruned
        """
        msg_of_sub = set()
        assert isinstance(self.queue, persipubsub.queue._Queue)
        assert isinstance(self.queue.env, lmdb.Environment)
        with self.queue.env.begin(write=True) as txn:
            sub_db = self.queue.env.open_db(
                key=persipubsub.str_to_bytes(sub_id), txn=txn, create=False)

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

    def _remove_sub(self, sub_id: str) -> None:
        """
        Remove a subscriber and delete all its messages.

        :param sub_id: ID of the subscriber which should be removed
        """
        msg_of_sub = set()
        with self.env.begin(write=True) as txn:
            sub_db = self.env.open_db(
                key=persipubsub.str_to_bytes(sub_id), txn=txn, create=False)
            cursor = txn.cursor(db=sub_db)
            # check if database is not empty
            if cursor.first():
                for key in cursor.iternext(keys=True, values=False):
                    msg_of_sub.add(key)
            txn.drop(db=sub_db)

            pending_db = self.env.open_db(
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

            queue_db = self.env.open_db(
                persipubsub.QUEUE_DB, txn=txn, create=False)
            subscriber_ids = txn.get(
                key=persipubsub.SUBSCRIBER_IDS_KEY, db=queue_db)

            subscriber_list = persipubsub.bytes_to_str(
                encoded_str=subscriber_ids).split(' ')
            subscriber_set = set(subscriber_list)
            subscriber_set.remove(sub_id)
            subscriber_str = " ".join(subscriber_set)
            txn.put(
                key=persipubsub.SUBSCRIBER_IDS_KEY,
                value=subscriber_str.encode(persipubsub.ENCODING),
                db=queue_db)

            self.subscriber_ids.remove(sub_id)
