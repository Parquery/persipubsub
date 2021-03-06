#!/usr/bin/env python
"""Test persipubsub live."""
import multiprocessing
import pathlib
import threading
import time
import unittest

import temppathlib

import persipubsub.database
import persipubsub.environment
import persipubsub.publisher
import persipubsub.queue
import persipubsub.subscriber
import tests
import tests.component_publisher
import tests.component_subscriber

# pylint: disable=missing-docstring

FIRST_SUB_STARTED_READING = False


def subscriber_receive_first(sub: persipubsub.subscriber.Subscriber) -> None:
    with sub.receive() as _:
        global FIRST_SUB_STARTED_READING  # pylint: disable=global-statement
        FIRST_SUB_STARTED_READING = True
        time.sleep(2)


def subscriber_receive_second(sub: persipubsub.subscriber.Subscriber) -> None:
    global FIRST_SUB_STARTED_READING  # pylint: disable=global-statement

    start = time.time()
    while not FIRST_SUB_STARTED_READING:
        if time.time() - start > 10:
            raise TimeoutError
        time.sleep(0.1)

    with sub.receive() as _:
        if time.time() - start < 2:
            assert sub.queue is not None
            assert sub.queue.env is not None
            assert sub.queue.env.path() is not None
            assert sub.identifier is not None
            result = pathlib.Path(sub.queue.env.path()) / sub.identifier
            result.write_text('pass')
        else:
            assert sub.queue is not None
            assert sub.queue.env is not None
            assert sub.queue.env.path() is not None
            assert sub.identifier is not None
            result = pathlib.Path(sub.queue.env.path()) / sub.identifier
            result.write_text('fail')


def receive(sub: persipubsub.subscriber.Subscriber,
            num_msg: int,
            timeout: int = 2,
            method_timeout: int = 60) -> None:
    received_msg = 0

    start = time.time()
    while True:
        if received_msg == num_msg:
            break
        elif time.time() - start >= method_timeout:
            raise TimeoutError

        with sub.receive(timeout=timeout) as msg:
            if msg is not None:
                received_msg += 1

    assert sub.queue is not None
    assert sub.queue.env is not None
    assert sub.queue.env.path() is not None
    assert sub.identifier is not None
    result = pathlib.Path(sub.queue.env.path()) / sub.identifier
    result.write_text('pass')


def send(pub: persipubsub.publisher.Publisher, num_msg: int) -> None:
    for _ in range(num_msg):
        pub.send(msg="hello subscriber".encode('utf-8'))


class TestLive(unittest.TestCase):
    def test_multithreaded_communication_one_publisher_one_subscriber(
            self) -> None:
        with temppathlib.TemporaryDirectory() as tmp_dir:
            env = persipubsub.environment.initialize(path=tmp_dir.path)
            _ = env.new_control({'sub'})

            result = tmp_dir.path / "sub"
            result.touch()

            pub = env.new_publisher()
            sub = env.new_subscriber(identifier='sub')

            num_msg = 1000
            pub_thread = threading.Thread(
                target=send, kwargs={
                    'pub': pub,
                    'num_msg': num_msg
                })
            sub_thread = threading.Thread(
                target=receive, kwargs={
                    'sub': sub,
                    'num_msg': num_msg
                })

            pub_thread.start()
            sub_thread.start()

            for thread in [pub_thread, sub_thread]:
                thread.join()

            self.assertEqual('pass', result.read_text())

    # pylint: disable=too-many-locals
    def test_multithreaded_communication_two_publisher_two_subscriber(
            self) -> None:
        with temppathlib.TemporaryDirectory() as tmp_dir:
            env = persipubsub.environment.initialize(path=tmp_dir.path)
            _ = env.new_control({'sub1', 'sub2'})

            result1 = tmp_dir.path / "sub1"
            result1.touch()
            result2 = tmp_dir.path / "sub2"
            result2.touch()

            pub1 = env.new_publisher()
            pub2 = env.new_publisher()
            sub1 = env.new_subscriber(identifier='sub1')
            sub2 = env.new_subscriber(identifier='sub2')

            num_msg = 1000

            pub1_thread = threading.Thread(
                target=send, kwargs={
                    'pub': pub1,
                    'num_msg': num_msg
                })
            sub1_thread = threading.Thread(
                target=receive, kwargs={
                    'sub': sub1,
                    'num_msg': 2 * num_msg
                })
            pub2_thread = threading.Thread(
                target=send, kwargs={
                    'pub': pub2,
                    'num_msg': num_msg
                })
            sub2_thread = threading.Thread(
                target=receive, kwargs={
                    'sub': sub2,
                    'num_msg': 2 * num_msg
                })

            pub1_thread.start()
            sub1_thread.start()
            pub2_thread.start()
            sub2_thread.start()

            for thread in [pub1_thread, sub1_thread, pub2_thread, sub2_thread]:
                thread.join()

            self.assertEqual('pass', result1.read_text())
            self.assertEqual('pass', result2.read_text())

    def test_multithreaded_component_publisher_component_subscriber(
            self) -> None:
        with temppathlib.TemporaryDirectory() as tmp_dir:
            env = persipubsub.environment.initialize(path=tmp_dir.path)
            _ = env.new_control({'sub'})

            result = tmp_dir.path / "sub"
            result.touch()

            num_msg = 1000
            pub_thread = threading.Thread(
                target=tests.component_publisher.send_thread,
                kwargs={
                    'env': env,
                    'num_msg': num_msg
                })
            sub_thread = threading.Thread(
                target=tests.component_subscriber.receive_thread,
                kwargs={
                    'path': tmp_dir.path,
                    'env': env,
                    'identifier': 'sub',
                    'num_msg': num_msg
                })
            pub_thread.start()
            sub_thread.start()

            for thread in [pub_thread, sub_thread]:
                thread.join()

            self.assertEqual('pass', result.read_text())

    def test_multithreaded_two_component_publisher_component_subscriber(
            self) -> None:
        with temppathlib.TemporaryDirectory() as tmp_dir:
            env = persipubsub.environment.initialize(path=tmp_dir.path)
            _ = env.new_control({'sub1', 'sub2'})

            result1 = tmp_dir.path / "sub1"
            result1.touch()
            result2 = tmp_dir.path / "sub2"
            result2.touch()

            num_msg = 300
            pub1_thread = threading.Thread(
                target=tests.component_publisher.send_thread,
                kwargs={
                    'env': env,
                    'num_msg': num_msg
                })
            sub1_thread = threading.Thread(
                target=tests.component_subscriber.receive_thread,
                kwargs={
                    'path': tmp_dir.path,
                    'env': env,
                    'identifier': 'sub1',
                    'num_msg': 2 * num_msg,
                    'method_timeout': 60
                })
            pub2_thread = threading.Thread(
                target=tests.component_publisher.send_thread,
                kwargs={
                    'env': env,
                    'num_msg': num_msg
                })
            sub2_thread = threading.Thread(
                target=tests.component_subscriber.receive_thread,
                kwargs={
                    'path': tmp_dir.path,
                    'env': env,
                    'identifier': 'sub2',
                    'num_msg': 2 * num_msg,
                    'method_timeout': 60
                })
            pub1_thread.start()
            sub1_thread.start()
            pub2_thread.start()
            sub2_thread.start()

            for thread in [pub1_thread, sub1_thread, pub2_thread, sub2_thread]:
                thread.join()

            self.assertEqual('pass', result1.read_text())
            self.assertEqual('pass', result2.read_text())

    def test_multiprocess_component_publisher_component_subscriber(
            self) -> None:
        with temppathlib.TemporaryDirectory() as tmp_dir:
            env = persipubsub.environment.initialize(path=tmp_dir.path)
            _ = env.new_control({'sub'})
            env._env.close()  # pylint: disable=protected-access

            result = tmp_dir.path / "sub"
            result.touch()

            num_msg = 1000
            pub_process = multiprocessing.Process(
                target=tests.component_publisher.send_process,
                kwargs={
                    'path': tmp_dir.path,
                    'num_msg': num_msg
                })
            sub_process = multiprocessing.Process(
                target=tests.component_subscriber.receive_process,
                kwargs={
                    'path': tmp_dir.path,
                    'identifier': 'sub',
                    'num_msg': num_msg
                })
            pub_process.start()
            sub_process.start()

            for process in [pub_process, sub_process]:
                process.join()

            self.assertEqual('pass', result.read_text())

    def test_multiprocess_two_component_publisher_component_subscriber(
            self) -> None:
        with temppathlib.TemporaryDirectory() as tmp_dir:
            env = persipubsub.environment.initialize(path=tmp_dir.path)
            _ = env.new_control({'sub1', 'sub2'})

            result1 = tmp_dir.path / "sub1"
            result1.touch()
            result2 = tmp_dir.path / "sub2"
            result2.touch()

            num_msg = 300
            pub1_process = multiprocessing.Process(
                target=tests.component_publisher.send_process,
                kwargs={
                    'path': tmp_dir.path,
                    'num_msg': num_msg
                })
            sub1_process = multiprocessing.Process(
                target=tests.component_subscriber.receive_process,
                kwargs={
                    'path': tmp_dir.path,
                    'identifier': 'sub1',
                    'num_msg': 2 * num_msg,
                    'method_timeout': 60
                })
            pub2_process = multiprocessing.Process(
                target=tests.component_publisher.send_process,
                kwargs={
                    'path': tmp_dir.path,
                    'num_msg': num_msg
                })
            sub2_process = multiprocessing.Process(
                target=tests.component_subscriber.receive_process,
                kwargs={
                    'path': tmp_dir.path,
                    'identifier': 'sub2',
                    'num_msg': 2 * num_msg,
                    'method_timeout': 60
                })
            pub1_process.start()
            sub1_process.start()
            pub2_process.start()
            sub2_process.start()

            for process in [
                    pub1_process, sub1_process, pub2_process, sub2_process
            ]:
                process.join()

            self.assertEqual('pass', result1.read_text())
            self.assertEqual('pass', result2.read_text())

    def test_multithreaded_race_condition_of_the_component_publisher(
            self) -> None:
        with temppathlib.TemporaryDirectory() as tmp_dir:
            env = persipubsub.environment.initialize(path=tmp_dir.path)
            control = env.new_control({'sub'})

            num_msg = 50
            num_threads = 50

            threads = []
            for _ in range(num_threads):
                pub_thread = threading.Thread(
                    target=tests.component_publisher.send_thread,
                    kwargs={
                        'env': env,
                        'num_msg': num_msg
                    })

                pub_thread.start()
                threads.append(pub_thread)

            for thread in threads:
                thread.join()

            assert control.queue is not None  # pylint: disable=protected-access
            assert control.queue.env is not None
            with control.queue.env.begin(write=False) as txn:
                sub_db = control.queue.env.open_db(
                    key='sub'.encode('utf-8'), txn=txn)
                self.assertEqual(num_msg * num_threads,
                                 txn.stat(db=sub_db)['entries'])

    def test_multiprocess_race_condition_of_the_component_publisher(
            self) -> None:
        with temppathlib.TemporaryDirectory() as tmp_dir:
            env = persipubsub.environment.initialize(path=tmp_dir.path)
            control = env.new_control({'sub'})

            num_msg = 50
            num_processes = 50

            processes = []
            for _ in range(num_processes):
                pub_process = multiprocessing.Process(
                    target=tests.component_publisher.send_process,
                    kwargs={
                        'path': tmp_dir.path,
                        'num_msg': num_msg
                    })

                pub_process.start()
                processes.append(pub_process)

            for process in processes:
                process.join()

            assert control.queue is not None  # pylint: disable=protected-access
            assert control.queue.env is not None
            with control.queue.env.begin(write=False) as txn:
                sub_db = control.queue.env.open_db(
                    key='sub'.encode('utf-8'), txn=txn)
                self.assertEqual(num_processes * num_msg,
                                 txn.stat(db=sub_db)['entries'])

    def test_multiprocess_race_condition_of_the_component_publisher_one_env(
            self) -> None:
        with temppathlib.TemporaryDirectory() as tmp_dir:
            env = persipubsub.environment.initialize(path=tmp_dir.path)
            control = env.new_control(subscriber_ids={'sub'})

            num_msg = 50
            num_processes = 50

            processes = []
            for _ in range(num_processes):
                pub_process = multiprocessing.Process(
                    target=tests.component_publisher.send_process,
                    kwargs={
                        'num_msg': num_msg,
                        'path': tmp_dir.path,
                    })

                pub_process.start()
                processes.append(pub_process)

            for process in processes:
                process.join()

            assert control.queue is not None  # pylint: disable=protected-access
            assert control.queue.env is not None
            with control.queue.env.begin(write=False) as txn:
                sub_db = control.queue.env.open_db(
                    key='sub'.encode('utf-8'), txn=txn)
                self.assertEqual(num_processes * num_msg,
                                 txn.stat(db=sub_db)['entries'])
                data_db = control.queue.env.open_db(
                    key=persipubsub.database.DATA_DB, txn=txn)
                self.assertEqual(num_processes * num_msg,
                                 txn.stat(db=data_db)['entries'])
                meta_db = control.queue.env.open_db(
                    key=persipubsub.database.META_DB, txn=txn)
                self.assertEqual(num_processes * num_msg,
                                 txn.stat(db=meta_db)['entries'])
                pending_db = control.queue.env.open_db(
                    key=persipubsub.database.PENDING_DB, txn=txn)
                self.assertEqual(num_processes * num_msg,
                                 txn.stat(db=pending_db)['entries'])

    def test_2_subscriber_non_blocking(self) -> None:
        with temppathlib.TemporaryDirectory() as tmp_dir:
            env = persipubsub.environment.initialize(path=tmp_dir.path)
            _ = env.new_control(subscriber_ids={'sub1', 'sub2'})

            pub = env.new_publisher()
            sub1 = env.new_subscriber(identifier='sub1')
            sub2 = env.new_subscriber(identifier='sub2')

            pub.send(msg='msg for two subscriber'.encode('utf-8'))

            sub1_thread = threading.Thread(
                target=subscriber_receive_first, kwargs={
                    'sub': sub1,
                })
            sub2_thread = threading.Thread(
                target=subscriber_receive_second, kwargs={
                    'sub': sub2,
                })
            sub1_thread.start()
            sub2_thread.start()

            for thread in [sub1_thread, sub2_thread]:
                thread.join()

            assert sub2.queue is not None
            assert sub2.queue.env is not None
            assert sub2.queue.env.path() is not None
            assert sub2.identifier is not None
            result = pathlib.Path(sub2.queue.env.path()) / sub2.identifier
            self.assertEqual('pass', result.read_text())


if __name__ == '__main__':
    unittest.main()
