#!/usr/bin/env python
"""Distribute messages persistent from publisher to subscriber."""
import json
import pathlib
from typing import Dict, Union

# define all encodings here
ENCODING = 'utf-8'
BYTES_LENGTH = 8
BYTES_ORDER = 'big'

# define all database names here
DATA_DB = "data_db".encode(ENCODING)  # msg_id | data
PENDING_DB = "pending_db".encode(ENCODING)  # msg_id | pending subscriber
META_DB = "meta_db".encode(ENCODING)  # msg_id | metadata


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


def get_config(path: Union[pathlib.Path, str]) -> Dict[str, Dict[str, str]]:
    """
    Get queues, publishers and subscribers data.

    :param path: path to the JSON config file
    :return config data with queues, publishers and subscribers
    """
    pth = path if isinstance(path, pathlib.Path) else pathlib.Path(path)

    if not pth.is_file():
        raise FileNotFoundError("Config file at path '{}' not found".format(
            pth.as_posix()))

    with open(pth.as_posix(), mode="rt") as file_object:
        config = json.load(file_object)
        return config
