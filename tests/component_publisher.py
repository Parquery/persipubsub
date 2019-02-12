#!/usr/bin/env python
"""Publisher component for live test."""

import pathlib
import time

import persipubsub.environment
import persipubsub.publisher

# pylint: disable=missing-docstring


def send_thread(env: persipubsub.environment.Environment,
                num_msg: int,
                sleep_time: float = 0) -> None:
    pub = env.new_publisher()

    for _ in range(num_msg):
        pub.send(msg="hello subscriber".encode('utf-8'))
        time.sleep(sleep_time)


def send_process(path: pathlib.Path, num_msg: int,
                 sleep_time: float = 0) -> None:
    env = persipubsub.environment.Environment(path=path)
    pub = env.new_publisher()

    for _ in range(num_msg):
        pub.send(msg="hello subscriber".encode('utf-8'))
        time.sleep(sleep_time)
