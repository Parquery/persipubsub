#!/usr/bin/env python3
"""Setup and control a persistent queue."""

import pathlib
from typing import Any, List, Optional, Set

import icontract
import lmdb  # pylint: disable=unused-import

import persipubsub.database
import persipubsub.queue

# pylint: disable=protected-access


def initialize(path: pathlib.Path,
               env: Optional[lmdb.Environment] = None,
               subscriber_ids: Optional[Set[str]] = None,
               high_water_mark: persipubsub.queue.HighWaterMark = persipubsub.
               queue.HighWaterMark(),
               strategy: persipubsub.queue.Strategy = persipubsub.queue.
               Strategy.PRUNE_FIRST) -> 'Control':
    """
    Initialize a new control.

    :param path: to the LMDB
    :param env: LMDB environment
    :param subscriber_ids: subscribers of the queue
    :param high_water_mark: high water mark limit of the queue
    :param strategy: used to prune queue
    :return: Control to create and maintain queue
    """
    if not path.exists():
        path.mkdir(parents=True, exist_ok=True)

    if env is not None:
        _env = env
    else:
        _env = persipubsub.queue._initialize_environment(
            queue_dir=path,
            max_reader_num=persipubsub.database.DEFAULT_MAX_READERS,
            max_db_num=persipubsub.database.DEFAULT_MAX_DB_NUM,
            max_db_size_bytes=persipubsub.database.DEFAULT_MAX_DB_SIZE)

    control = Control(path=path, env=_env)
    control.init(
        subscriber_ids=subscriber_ids,
        high_water_mark=high_water_mark,
        strategy=strategy)
    return control


class Control:
    """Control and maintain a queue."""

    def __init__(self, path: pathlib.Path, env: lmdb.Environment):
        """
        Initialize control class.

        :param path: to the queue.
        :param env: open LMDB environment
        """
        self.path = path
        self.queue = None  # type: Optional[persipubsub.queue._Queue]
        self.subscriber_ids = set()  # type: Set[str]
        self.env = env
        self.closed = False

    def __enter__(self) -> 'Control':
        """Enter the context and give the ctl prepared in the constructor."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Leave the context."""
        self.close()

    def close(self) -> None:
        """Close control."""
        self.closed = True

    @icontract.require(lambda self: not self.closed)
    def init(self,
             subscriber_ids: Optional[Set[str]] = None,
             high_water_mark: persipubsub.queue.HighWaterMark = persipubsub.
             queue.HighWaterMark(),
             strategy: persipubsub.queue.Strategy = persipubsub.queue.Strategy.
             PRUNE_FIRST) -> None:
        """
        Initialize control with a (re)initialized queue.

        :param subscriber_ids: subscribers of the queue
        :param high_water_mark: high water mark limit of the queue
        :param strategy: used to prune queue
        :return:
        """
        # pylint: disable=too-many-arguments
        if self.is_initialized():
            self._reinitialize_queue()
        else:
            if subscriber_ids is None:
                subscriber_ids = set()
            assert subscriber_ids is not None
            self._initialize_queue(
                subscriber_ids=subscriber_ids,
                high_water_mark=high_water_mark,
                strategy=strategy)

    def _reinitialize_queue(self) -> None:
        """Reinitialize the queue which is maintained by the control."""
        self.queue = persipubsub.queue._Queue(
        )  # type: persipubsub.queue._Queue
        self.queue.init(path=self.path, env=self.env)
        assert self.queue.subscriber_ids is not None
        self.subscriber_ids = set(self.queue.subscriber_ids)

    @icontract.ensure(
        lambda self: self.is_initialized(), enabled=icontract.SLOW)
    def _initialize_queue(self,
                          subscriber_ids: Set[str],
                          high_water_mark: persipubsub.queue.
                          HighWaterMark = persipubsub.queue.HighWaterMark(),
                          strategy: persipubsub.queue.Strategy = persipubsub.
                          queue.Strategy.PRUNE_FIRST) -> None:
        """
        Initialize queue.

        :param subscriber_ids: subscribers of the queue
        :param high_water_mark: high water mark limit of the queue
        :param strategy: used to prune queue
        :return:
        """
        self.subscriber_ids = set(subscriber_ids)
        # Databases needed for queue:
        # 5 queues (data db, meta db, pending db, queue db, subscriber db)
        # + each subscriber has its own db

        self.set_hwm(hwm=high_water_mark)
        self.set_strategy(strategy=strategy)

        for sub in self.subscriber_ids:
            self._add_sub(sub_id=sub)

        # load initialized queue
        self.queue = persipubsub.queue._Queue(
        )  # type: persipubsub.queue._Queue
        self.queue.init(path=self.path, env=self.env)

    @icontract.require(lambda self: not self.closed)
    def is_initialized(self) -> bool:
        """
        Check if queue is initialized.

        :return: is initialized when all values for the given keys are set
        """
        try:
            _ = persipubsub.database.retrieve_queue_data(env=self.env)
        except lmdb.NotFoundError:
            return False

        return True

    @icontract.require(lambda self: not self.closed)
    @icontract.ensure(
        lambda self: self.env.stat()['entries'] == 5, enabled=icontract.SLOW)
    def clear_all_subscribers(self) -> None:
        """Clear all subscriber and delete all messages for queue."""
        assert self.queue is not None
        assert self.queue.env is not None
        with self.queue.env.begin(write=True) as txn:
            for sub_id in self.subscriber_ids:
                try:
                    sub_db = self.queue.env.open_db(
                        key=persipubsub.database.str_to_bytes(sub_id),
                        txn=txn,
                        create=False)
                    txn.drop(db=sub_db)
                except lmdb.NotFoundError:
                    pass

            pending_db = self.queue.env.open_db(
                key=persipubsub.database.PENDING_DB, txn=txn, create=False)
            txn.drop(db=pending_db, delete=False)
            meta_db = self.queue.env.open_db(
                key=persipubsub.database.META_DB, txn=txn, create=False)
            txn.drop(db=meta_db, delete=False)
            data_db = self.queue.env.open_db(
                key=persipubsub.database.DATA_DB, txn=txn, create=False)
            txn.drop(db=data_db, delete=False)

    @icontract.require(lambda self: not self.closed)
    def prune_dangling_messages(self) -> None:
        """Prune all dangling messages from the LMDB."""
        assert self.queue is not None
        persipubsub.queue._prune_dangling_messages_for(
            queue=self.queue, subscriber_ids=set(self.subscriber_ids))

    @icontract.ensure(
        lambda self, sub_id: not self._has_messages_for(sub_id=sub_id),
        enabled=icontract.SLOW)
    def _prune_all_messages_for(self, sub_id: str) -> None:
        """
        Prune all messages of a subscriber.

        :param sub_id:
            Identifier of the subscriber whose messages should be pruned
        """
        # pylint: disable=too-many-locals
        message_ids = set()  # type: Set[bytes]
        assert self.queue is not None
        assert self.queue.env is not None
        with self.queue.env.begin(write=True) as txn:
            sub_db = self.queue.env.open_db(
                key=persipubsub.database.str_to_bytes(sub_id),
                txn=txn,
                create=False)

            cursor = txn.cursor(db=sub_db)
            # check if database is not empty
            if cursor.first():
                for key in cursor.iternext(keys=True, values=False):
                    message_ids.add(key)

            txn.drop(db=sub_db, delete=False)

            pending_db = self.queue.env.open_db(
                key=persipubsub.database.PENDING_DB, txn=txn, create=False)

            for key in message_ids:
                pending_value = txn.get(key=key, db=pending_db)
                pending_num = persipubsub.database.bytes_to_int(pending_value)
                decreased_pending_num = pending_num - 1
                assert decreased_pending_num >= 0
                txn.put(
                    key=key,
                    value=persipubsub.database.int_to_bytes(
                        decreased_pending_num),
                    db=pending_db)

    def _has_sub(self, sub_id: str) -> bool:
        """
        Check if subscriber is properly initialized in the queue.

        :param sub_id: identifier of the subscriber
        :return: initialization status of subscriber
        """
        with self.env.begin(write=False) as txn:
            has_own_db = True
            in_sub_set = sub_id in self.subscriber_ids
            try:
                _ = self.env.open_db(
                    key=persipubsub.database.str_to_bytes(sub_id),
                    txn=txn,
                    create=False)
            except lmdb.NotFoundError:
                has_own_db = False

        return has_own_db or in_sub_set

    @icontract.ensure(
        lambda self, sub_id: self._has_sub(sub_id=sub_id),
        enabled=icontract.SLOW)
    def _add_sub(self, sub_id: str) -> None:
        """
        Add a subscriber and create its corresponding database.

        :param sub_id: identifier of the subscriber which should be added
        """
        with self.env.begin(write=True) as txn:
            _ = self.env.open_db(
                key=persipubsub.database.str_to_bytes(sub_id),
                txn=txn,
                create=True)

            subscriber_db = self.env.open_db(
                persipubsub.database.SUBSCRIBER_DB, txn=txn, create=True)
            txn.put(
                key=persipubsub.database.str_to_bytes(sub_id), db=subscriber_db)

    @icontract.ensure(
        lambda self, sub_id: not self._has_sub(sub_id=sub_id),
        enabled=icontract.SLOW)
    def _remove_sub(self, sub_id: str) -> None:
        """
        Remove a subscriber and delete all its messages.

        When the subscriber does not exist in the database then the
        post-condition is already met and nothing happens.

        :param sub_id: identifier of the subscriber which should be removed
        """
        # pylint: disable=too-many-locals
        msg_of_sub = set()  # type: Set[bytes]
        with self.env.begin(write=True) as txn:
            try:
                sub_db = self.env.open_db(
                    key=persipubsub.database.str_to_bytes(sub_id),
                    txn=txn,
                    create=False)

                cursor = txn.cursor(db=sub_db)
                # check if database is not empty
                if cursor.first():
                    for key in cursor.iternext(keys=True, values=False):
                        msg_of_sub.add(key)
                txn.drop(db=sub_db)

            except lmdb.NotFoundError:
                # Subscriber doesn't exist. Post condition is met.
                pass

            pending_db = self.env.open_db(
                key=persipubsub.database.PENDING_DB, txn=txn, create=False)

            for key in msg_of_sub:
                pending_value = txn.get(key=key, db=pending_db)
                pending_num = persipubsub.database.bytes_to_int(pending_value)
                decreased_pending_num = pending_num - 1
                assert decreased_pending_num >= 0
                txn.put(
                    key=key,
                    value=persipubsub.database.int_to_bytes(
                        decreased_pending_num),
                    db=pending_db)

            subscriber_db = self.env.open_db(
                persipubsub.database.SUBSCRIBER_DB, txn=txn, create=False)

            try:
                txn.delete(
                    key=persipubsub.database.str_to_bytes(sub_id),
                    db=subscriber_db)
            except lmdb.NotFoundError:
                # Subscriber doesn't exist. Post condition is met.
                pass

            if sub_id in self.subscriber_ids:
                self.subscriber_ids.remove(sub_id)

    @icontract.require(lambda self: not self.closed)
    def set_hwm(self, hwm: persipubsub.queue.HighWaterMark) -> None:
        """
        Set high water mark values for queue.

        | The high water mark defaults are:
        |   messages timeout after 500 secs,
        |   maximal number of messages are 64 * 1024,
        |   the hwm size of LMDB is ~30 GB

        In order to change the high water mark on an existing queue, you need
        to open a new persipubsub environment.

        Messages are pruned when high water mark is reached and a publisher
        puts a new message in the queue.

        :param hwm: high water mark values
        :return:
        """
        with self.env.begin(write=True) as txn:
            queue_db = self.env.open_db(
                persipubsub.database.QUEUE_DB, txn=txn, create=True)
            txn.put(
                key=persipubsub.database.HWM_DB_SIZE_KEY,
                value=persipubsub.database.int_to_bytes(hwm.hwm_lmdb_size),
                db=queue_db)
            txn.put(
                key=persipubsub.database.MAX_MESSAGES_KEY,
                value=persipubsub.database.int_to_bytes(hwm.max_messages),
                db=queue_db)
            txn.put(
                key=persipubsub.database.MESSAGE_TIMEOUT_KEY,
                value=persipubsub.database.int_to_bytes(hwm.message_timeout),
                db=queue_db)

    @icontract.require(lambda self: not self.closed)
    def set_strategy(self, strategy: persipubsub.queue.Strategy) -> None:
        """
        Set pruning strategy for queue.

        In order to change the strategy on an existing queue, you need
        to open a new persipubsub environment.

        Messages are pruned when high water mark is reached and a publisher
        puts a new message in the queue.

        The default strategy to prune messages is prune_first, which means that
        the oldest messages are dropped.

        :param strategy: pruning strategy
        :return:
        """
        with self.env.begin(write=True) as txn:
            queue_db = self.env.open_db(
                persipubsub.database.QUEUE_DB, txn=txn, create=True)
            txn.put(
                key=persipubsub.database.STRATEGY_KEY,
                value=persipubsub.database.str_to_bytes(str(strategy.name)),
                db=queue_db)

    def _has_messages_for(self, sub_id: str) -> bool:
        with self.env.begin(write=False) as txn:
            sub_db = self.env.open_db(
                key=persipubsub.database.str_to_bytes(sub_id), txn=txn)
            stat = txn.stat(db=sub_db)

            return not stat['entries'] == 0
