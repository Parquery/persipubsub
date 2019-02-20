#!/usr/bin/env python
"""Publisher component for live test."""

import pathlib

import persipubsub.environment

# pylint: disable=missing-docstring


def send_thread(env: persipubsub.environment.Environment, num_msg: int) -> None:
    pub = env.new_publisher()

    for _ in range(num_msg):
        pub.send(msg="hello subscriber".encode('utf-8'))


def send_process(path: pathlib.Path, num_msg: int) -> None:
    env = persipubsub.environment.Environment(path=path)
    pub = env.new_publisher()

    for _ in range(num_msg):
        pub.send(msg="hello subscriber".encode('utf-8'))
