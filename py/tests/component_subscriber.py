#!/usr/bin/env python
"""Subscriber component for live test."""

import pathlib
import time

import persipubsub.environment

# pylint: disable=missing-docstring


# pylint: disable=too-many-arguments
def receive_thread(path: pathlib.Path,
                   env: persipubsub.environment.Environment,
                   identifier: str,
                   num_msg: int,
                   timeout: int = 2,
                   retries: int = 10,
                   method_timeout: int = 10) -> None:
    sub = env.new_subscriber(identifier=identifier)

    received_msg = 0

    start = time.time()
    while True:
        if received_msg == num_msg:
            break
        elif time.time() - start >= method_timeout:
            raise TimeoutError
        else:
            with sub.receive(timeout=timeout, retries=retries) as msg:
                if msg is not None:
                    received_msg += 1

    result = path / identifier
    result.write_text('pass')


# pylint: disable=too-many-arguments
def receive_process(path: pathlib.Path,
                    identifier: str,
                    num_msg: int,
                    timeout: int = 2,
                    retries: int = 10,
                    method_timeout: int = 60) -> None:
    env = persipubsub.environment.Environment(path=path)
    sub = env.new_subscriber(identifier=identifier)

    received_msg = 0

    start = time.time()
    while True:
        if received_msg == num_msg:
            break
        elif time.time() - start >= method_timeout:
            raise TimeoutError
        else:
            with sub.receive(timeout=timeout, retries=retries) as msg:
                if msg is not None:
                    received_msg += 1

    result = path / identifier
    result.write_text('pass')
