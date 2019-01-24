#!/usr/bin/env python
"""Setup and control persistent distribution of messages."""

import pathlib
from typing import Any, Dict, List, Union

import persipubsub.queue


def initialize_all_dbs(config_pth: Union[pathlib.Path, str]) -> None:
    """
    Initialize all needed queues.

    :param config_pth: path to the JSON config file
    """
    config = persipubsub.get_config(path=config_pth)  # type: Dict[str, Any]
    queues = config["queues"]  # type: Dict[str, Any]
    # TODO(snaji): maybe multi-threaded especially different queues
    for values in queues.values():
        queue_dir = pathlib.Path(values["path"])
        if not queue_dir.exists():
            queue_dir.mkdir(parents=True, exist_ok=True)
        sub_list = values["subscribers"]
        queue = persipubsub.queue.Queue()
        queue.init(
            config_pth=config_pth,
            queue_dir=queue_dir,
            max_reader_num=values["max_reader_num"],
            max_db_num=values["max_db_num"],
            max_db_size_bytes=values["max_db_size_bytes"])

        for sub in sub_list:
            add_sub(sub_id=sub, queue=queue)


def clear_all_subs(config_pth: Union[pathlib.Path, str]) -> None:
    """
    Clear all subscriber and delete all messages.

    :param config_pth: path to the JSON config file
    """
    config = persipubsub.get_config(path=config_pth)  # type: Dict[str, Any]
    queues = config["queues"]  # type: Dict[str, Any]
    for values in queues.values():
        queue_dir = pathlib.Path(values["path"])
        if not queue_dir.exists():
            queue_dir.mkdir(parents=True, exist_ok=True)
        sub_list = values["subscribers"]  # type: List[str]
        queue = persipubsub.queue.Queue()
        queue.init(
            config_pth=config_pth,
            queue_dir=queue_dir,
            max_reader_num=values["max_reader_num"],
            max_db_num=values["max_db_num"],
            max_db_size_bytes=values["max_db_size_bytes"])
        clear_all_subs_for(queue=queue, sub_list=sub_list)


def clear_all_subs_for(queue: persipubsub.queue.Queue,
                       sub_list: List[str]) -> None:
    """
    Clear all subscriber and delete all messages for one queue.

    :param queue: which will be cleared
    :param sub_list: subscribers which will be removed
    """
    with queue.env.begin(write=True) as txn:
        for sub_id in sub_list:
            sub_db = queue.env.open_db(
                key=persipubsub.encoding(sub_id), txn=txn, create=False)
            txn.drop(db=sub_db)

            pending_db = queue.env.open_db(
                key=persipubsub.PENDING_DB, txn=txn, create=False)
            txn.drop(db=pending_db, delete=False)
            meta_db = queue.env.open_db(
                key=persipubsub.META_DB, txn=txn, create=False)
            txn.drop(db=meta_db, delete=False)
            data_db = queue.env.open_db(
                key=persipubsub.DATA_DB, txn=txn, create=False)
            txn.drop(db=data_db, delete=False)


def add_sub(sub_id: str, queue: persipubsub.queue.Queue) -> None:
    """
    Add a subscriber and create its lmdb.

    :param sub_id: ID of the subscriber which should be added
    :param queue: to which the subscriber is added
    """
    with queue.env.begin(write=True) as txn:
        _ = queue.env.open_db(
            key=persipubsub.encoding(sub_id), txn=txn, create=True)


def remove_sub(sub_id: str, queue: persipubsub.queue.Queue) -> None:
    """
    Remove a subscriber and delete all its messages.

    :param sub_id: ID of the subscriber which should be removed
    :param queue: from which the subscriber is removed
    """
    msg_of_sub = set()
    with queue.env.begin(write=True) as txn:
        sub_db = queue.env.open_db(
            key=persipubsub.encoding(sub_id), txn=txn, create=False)
        # TODO(snaji): messages in pending_db?
        cursor = txn.cursor(db=sub_db)
        # check if database is not empty
        if cursor.first():
            for key in cursor.iternext(keys=True, values=False):
                msg_of_sub.add(key)
        txn.drop(db=sub_db)

        pending_db = queue.env.open_db(
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


def prune_dangling_messages(config_pth: Union[pathlib.Path, str]) -> None:
    """
    Prune all dangling messages from the lmdb.

    :param config_pth: path to the JSON config file
    """
    config = persipubsub.get_config(path=config_pth)  # type: Dict[str, Any]
    queues = config["queues"]  # type: Dict[str, Any]
    for values in queues.values():
        queue_dir = pathlib.Path(values["path"])
        if not queue_dir.exists():
            queue_dir.mkdir(parents=True, exist_ok=True)
        sub_list = values["subscribers"]  # type: List[str]
        queue = persipubsub.queue.Queue()
        queue.init(
            config_pth=config_pth,
            queue_dir=queue_dir,
            max_reader_num=values["max_reader_num"],
            max_db_num=values["max_db_num"],
            max_db_size_bytes=values["max_db_size_bytes"])
        persipubsub.queue.prune_dangling_messages_for(
            queue=queue, sub_list=sub_list)


def prune_all_messages_for(sub_id: str, queue: persipubsub.queue.Queue) -> None:
    """
    Prune all messages of a subscriber.

    :param sub_id: ID of the subscriber of which all messages should be pruned
    :param queue: of which some subscriber's messages should be pruned
    """
    # TODO(snaji): pending_db?
    msg_of_sub = set()
    with queue.env.begin(write=True) as txn:
        sub_db = queue.env.open_db(
            key=persipubsub.encoding(sub_id), txn=txn, create=False)

        cursor = txn.cursor(db=sub_db)
        # check if database is not empty
        if cursor.first():
            for key in cursor.iternext(keys=True, values=False):
                msg_of_sub.add(key)

        txn.drop(db=sub_db, delete=False)

        pending_db = queue.env.open_db(
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
