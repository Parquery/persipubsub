#!/usr/bin/env python
"""Test environment."""

import unittest

import temppathlib

import persipubsub.control
import persipubsub.environment
import persipubsub.publisher
import persipubsub.queue
import persipubsub.subscriber

# pylint: disable=missing-docstring
# pylint: disable=protected-access


class TestEnvironment(unittest.TestCase):
    def test_new_environment(self) -> None:
        with temppathlib.TemporaryDirectory() as tmp_dir:
            env = persipubsub.environment.initialize(path=tmp_dir.path)
            self.assertIsInstance(env, persipubsub.environment.Environment)

    def test_new_control(self) -> None:
        with temppathlib.TemporaryDirectory() as tmp_dir:
            env = persipubsub.environment.initialize(path=tmp_dir.path)
            ctl = env.new_control()
            self.assertIsInstance(ctl, persipubsub.control.Control)

    def test_new_publisher(self) -> None:
        with temppathlib.TemporaryDirectory() as tmp_dir:
            env = persipubsub.environment.initialize(path=tmp_dir.path)
            ctl = env.new_control()
            ctl.init()
            pub = env.new_publisher()
            self.assertIsInstance(pub, persipubsub.publisher.Publisher)

    def test_new_subscriber(self) -> None:
        with temppathlib.TemporaryDirectory() as tmp_dir:
            env = persipubsub.environment.initialize(path=tmp_dir.path)
            ctl = env.new_control()
            ctl.init()
            sub = env.new_subscriber(identifier="sub")
            self.assertIsInstance(sub, persipubsub.subscriber.Subscriber)


if __name__ == '__main__':
    unittest.main()
