#!/usr/bin/env python
"""Benchmark persipubsub."""

import concurrent.futures
import datetime
import time
from typing import Any, Dict, List, Set, Tuple

import matplotlib.pyplot as plt
import temppathlib

import persipubsub.control
import persipubsub.environment
import persipubsub.queue

ENCODING = 'utf-8'
DATA_DB = "data_db".encode(ENCODING)
PENDING_DB = "pending_db".encode(ENCODING)
META_DB = "meta_db".encode(ENCODING)
QUEUE_DB = "queue_db".encode(ENCODING)


def setup(env: persipubsub.environment.Environment,
          sub_set: Set[str]) -> persipubsub.control.Control:
    """Create an initialized control."""
    hwm = persipubsub.queue.HighWaterMark()
    strategy = persipubsub.queue.Strategy.PRUNE_FIRST

    control = env.new_control(
        subscriber_ids=sub_set, high_water_mark=hwm, strategy=strategy)

    return control


def plot(title: str, points: List[Tuple[List[float], List[float]]]) -> None:
    """
    Plot the benchmark graph.

    :param title:
    :param points:
    :return:
    """
    plt.plot(points[0][0], points[0][1], points[1][0], points[1][1])
    readable_title = title.split(' ')[0]
    plt.title(readable_title)
    plt.xlabel("# of messages")
    plt.ylabel("time (s)")
    plt.legend(('Total time 1KB', 'Total time 0.5MB'))
    # if you want to plot it
    # plt.show()
    plt.savefig(readable_title + ".png")


def plot_avg(title: str, points: List[Tuple[List[float], List[float]]]) -> None:
    """
    Plot the average benchmark graph.

    :param title:
    :param points:
    :return:
    """
    plt.plot(points[0][0], points[0][1], points[1][0], points[1][1])
    readable_title = title.split(' ')[0]
    plt.title(readable_title)
    plt.xlabel("# of messages")
    plt.ylabel("time (s)")
    plt.legend(('Average time per message 1KB',
                'Average time per message 0.5MB'))
    # if you want to plot it
    # plt.show()
    plt.savefig(readable_title + "avg.png")


class PlotList:
    """Handle data for plots."""

    def __init__(self) -> None:
        """Initialize."""
        # number of publisher | number of subscriber | size of msgs |
        # total time | average | number of msgs
        self.send = dict(
        )  # type: Dict[str, Tuple[List[float], List[float], List[float]]]
        self.send_many = dict(
        )  # type: Dict[str, Tuple[List[float], List[float], List[float]]]
        self.receive = dict(
        )  # type: Dict[str, Tuple[List[float], List[float], List[float]]]
        self.live = dict(
        )  # type: Dict[str, Tuple[List[float], List[float], List[float]]]

    def append_send(self, key: str, total: float, average: float,
                    msg_num: int) -> None:
        """
        Append data for the send plot.

        :param key: which encodes which setup was used
        :param total: time which was needed to run the benchmark
        :param average: time per message which was needed to run the benchmark
        :param msg_num: number of messages used during the benchmark
        :return:
        """
        if key in self.send.keys():
            send_tuple = self.send.get(key)
            assert send_tuple is not None
            send_tuple[0].append(total)
            send_tuple[1].append(average)
            send_tuple[2].append(msg_num)
            self.send[key] = send_tuple
        else:
            self.send[key] = ([total], [average], [msg_num])

    def append_send_many(self, key: str, total: float, average: float,
                         msg_num: int) -> None:
        """
        Append data for the send_many plot.

        :param key: which encodes which setup was used
        :param total: time which was needed to run the benchmark
        :param average: time per message which was needed to run the benchmark
        :param msg_num: number of messages used during the benchmark
        :return:
        """
        if key in self.send_many.keys():
            send_many_tuple = self.send_many.get(key)
            assert send_many_tuple is not None
            send_many_tuple[0].append(total)
            send_many_tuple[1].append(average)
            send_many_tuple[2].append(msg_num)
            self.send_many[key] = send_many_tuple
        else:
            self.send_many[key] = ([total], [average], [msg_num])

    def append_receive(self, key: str, total: float, average: float,
                       msg_num: int) -> None:
        """
        Append data for the receive plot.

        :param key: which encodes which setup was used
        :param total: time which was needed to run the benchmark
        :param average: time per message which was needed to run the benchmark
        :param msg_num: number of messages used during the benchmark
        :return:
        """
        if key in self.receive.keys():
            receive_tuple = self.receive.get(key)
            assert receive_tuple is not None
            receive_tuple[0].append(total)
            receive_tuple[1].append(average)
            receive_tuple[2].append(msg_num)
            self.receive[key] = receive_tuple
        else:
            self.receive[key] = ([total], [average], [msg_num])

    def append_live(self, key: str, total: float, average: float,
                    msg_num: int) -> None:
        """
        Append data for the live plot.

        :param key: which encodes which setup was used
        :param total: time which was needed to run the benchmark
        :param average: time per message which was needed to run the benchmark
        :param msg_num: number of messages used during the benchmark
        :return:
        """
        if key in self.live.keys():
            live_tuple = self.live.get(key)
            assert live_tuple is not None
            live_tuple[0].append(total)
            live_tuple[1].append(average)
            live_tuple[2].append(msg_num)
            self.live[key] = live_tuple
        else:
            self.live[key] = ([total], [average], [msg_num])


def run() -> None:
    """Run the benchmarks."""
    # pylint: disable=too-many-statements
    # pylint: disable=too-many-locals
    plot_list = PlotList()
    # add missing sample
    for publisher_num in [1, 10, 20]:
        for subscriber_num in [1, 10, 20]:
            date = datetime.datetime.utcnow()
            for msg_size in [1024, int(0.5 * 1024**2)]:
                for msgs_num in [1, 100, 500, 750, 1000]:
                    key = "{}|{}pub{}sub {})".format(date, publisher_num,
                                                     subscriber_num, msg_size)
                    send_total, send_average = benchmark_send(
                        msgs_num=msgs_num,
                        msg_size=msg_size,
                        publisher_num=publisher_num,
                        subscriber_num=subscriber_num)

                    plot_list.append_send(
                        key=key,
                        total=send_total,
                        average=send_average,
                        msg_num=msgs_num)

                    send_many_total, send_many_average = benchmark_send_many(
                        msgs_num=msgs_num,
                        msg_size=msg_size,
                        publisher_num=publisher_num,
                        subscriber_num=subscriber_num)

                    plot_list.append_send_many(
                        key=key,
                        total=send_many_total,
                        average=send_many_average,
                        msg_num=msgs_num)

                    receive_total, receive_average = benchmark_receive(
                        msgs_num=msgs_num,
                        msg_size=msg_size,
                        publisher_num=publisher_num,
                        subscriber_num=subscriber_num)

                    plot_list.append_receive(
                        key=key,
                        total=receive_total,
                        average=receive_average,
                        msg_num=msgs_num)

                    live_total, live_average = benchmark_live(
                        msgs_num=msgs_num,
                        msg_size=msg_size,
                        publisher_num=publisher_num,
                        subscriber_num=subscriber_num)

                    plot_list.append_live(
                        key=key,
                        total=live_total,
                        average=live_average,
                        msg_num=msgs_num)

    num_different_msg_sizes = 2
    counter = 0
    send_list = []  # type: List[Tuple[List[float], List[float]]]
    send_list_avg = []  # type: List[Tuple[List[float], List[float]]]
    send_many_list = []  # type: List[Tuple[List[float], List[float]]]
    send_many_list_avg = []  # type: List[Tuple[List[float], List[float]]]
    receive_list = []  # type: List[Tuple[List[float], List[float]]]
    receive_list_avg = []  # type: List[Tuple[List[float], List[float]]]
    live_list = []  # type: List[Tuple[List[float], List[float]]]
    live_list_avg = []  # type: List[Tuple[List[float], List[float]]]

    # sort dicts
    sorted_keys = sorted(plot_list.send)

    for key in sorted_keys:
        if counter % num_different_msg_sizes == 0:
            send_list = []
            send_list_avg = []
            send_many_list = []
            send_many_list_avg = []
            receive_list = []
            receive_list_avg = []
            live_list = []
            live_list_avg = []

        counter += 1

        send_list.append((plot_list.send[key][2], plot_list.send[key][0]))
        send_list_avg.append((plot_list.send[key][2], plot_list.send[key][1]))
        send_many_list.append((plot_list.send_many[key][2],
                               plot_list.send_many[key][0]))
        send_many_list_avg.append((plot_list.send_many[key][2],
                                   plot_list.send_many[key][1]))
        receive_list.append((plot_list.receive[key][2],
                             plot_list.receive[key][0]))
        receive_list_avg.append((plot_list.receive[key][2],
                                 plot_list.receive[key][1]))
        live_list.append((plot_list.live[key][2], plot_list.live[key][0]))
        live_list_avg.append((plot_list.live[key][2], plot_list.live[key][1]))

        print(key)
        if counter % num_different_msg_sizes == 0:
            plot(title="send:" + key, points=send_list)
            plot_avg(title="send:" + key, points=send_list_avg)
            plot(title="send_many:" + key, points=send_many_list)
            plot_avg(title="send_many:" + key, points=send_many_list_avg)
            plot(
                title="receive:" + key,
                points=receive_list,
            )
            plot_avg(title="receive:" + key, points=receive_list_avg)
            plot(title="live:" + key, points=live_list)
            plot_avg(title="live:" + key, points=live_list_avg)


def send(msgs_num: int, msg_size: int,
         tmp_dir: temppathlib.TemporaryDirectory) -> float:
    """
    Send messages given the requirements.

    :param msgs_num: number of messages
    :param msg_size: size of messages (in bytes)
    :param tmp_dir: Temporary directory of the queue
    :return: running time
    """
    assert tmp_dir.path is not None
    env_process = persipubsub.environment.Environment(path=tmp_dir.path)
    pub = env_process.new_publisher()

    msg = ("a" * msg_size).encode(ENCODING)

    start = time.time()
    for _ in range(msgs_num):
        pub.send(msg=msg)
    end = time.time()
    total_time_process = end - start
    return total_time_process


def send_many(msgs_num: int, msg_size: int,
              tmp_dir: temppathlib.TemporaryDirectory) -> float:
    """
    Send_many messages given the requirements.

    :param msgs_num: number of messages
    :param msg_size: size of messages (in bytes)
    :param tmp_dir: Temporary directory of the queue
    :return: running time
    """
    assert tmp_dir.path is not None
    env_process = persipubsub.environment.Environment(path=tmp_dir.path)
    pub = env_process.new_publisher()

    msgs = [("a" * msg_size).encode(ENCODING) for _ in range(msgs_num)]

    start = time.time()
    pub.send_many(msgs=msgs)
    end = time.time()
    total_time_process = end - start
    return total_time_process


def receive(msgs_num: int, identifier: str,
            tmp_dir: temppathlib.TemporaryDirectory) -> None:
    """
    Receive as many messages as expected for given subscriber.

    :param msgs_num: Number of expected messages
    :param identifier: subscriber id
    :param tmp_dir: Temporary directory of the queue
    :return:
    """
    count = 0
    assert tmp_dir.path is not None
    env = persipubsub.environment.Environment(path=tmp_dir.path)
    sub = env.new_subscriber(identifier)
    while count < msgs_num:
        with sub.receive() as msg:
            if msg is not None:
                count += 1


def benchmark_send(msgs_num: int, msg_size: int, publisher_num: int,
                   subscriber_num: int) -> Tuple[float, float]:
    """
    Run benchmark for send with given configurations.

    :param msgs_num: number of messages
    :param msg_size: size of messages (in bytes)
    :param publisher_num: number of publishers
    :param subscriber_num: number of subscribers
    :return: Total runtime + average time per message
    """
    # pylint: disable=too-many-locals
    with temppathlib.TemporaryDirectory() as tmp_dir:
        env = persipubsub.environment.Environment(path=tmp_dir.path)
        sub_set = {"sub{}".format(index) for index in range(subscriber_num)}
        _ = setup(env=env, sub_set=sub_set)

        processes = []  # type: List[Any]

        start = time.time()
        with concurrent.futures.ProcessPoolExecutor() as executor:

            for index in range(publisher_num):
                send_process = executor.submit(
                    fn=send,
                    msgs_num=msgs_num,
                    msg_size=msg_size,
                    tmp_dir=tmp_dir)
                processes.append(send_process)

            for process in processes:
                process.result()

            end = time.time()

            for index in range(subscriber_num):
                receive_process = executor.submit(
                    fn=receive,
                    msgs_num=msgs_num * publisher_num,
                    identifier='sub{}'.format(index),
                    tmp_dir=tmp_dir)

                processes.append(receive_process)

            for process in processes:
                process.result()

        total_time = end - start
        average_time_per_msg = total_time / (msgs_num * publisher_num)
        print("Send: {}|{}|{}|{} \t| Total time: {} \t| "
              "Average time per message: {}".format(
                  msgs_num, msg_size, publisher_num, subscriber_num,
                  round(total_time, 2), round(average_time_per_msg, 4)))

    return total_time, average_time_per_msg


def benchmark_send_many(msgs_num: int, msg_size: int, publisher_num: int,
                        subscriber_num: int) -> Tuple[float, float]:
    """
    Run benchmark for send_many with given configurations.

    :param msgs_num: number of messages
    :param msg_size: size of messages (in bytes)
    :param publisher_num: number of publishers
    :param subscriber_num: number of subscribers
    :return: Total runtime + average time per message
    """
    # pylint: disable=too-many-locals
    with temppathlib.TemporaryDirectory() as tmp_dir:
        env = persipubsub.environment.Environment(path=tmp_dir.path)
        sub_set = {"sub{}".format(index) for index in range(subscriber_num)}
        _ = setup(env=env, sub_set=sub_set)

        processes = []  # type: List[Any]

        start = time.time()
        with concurrent.futures.ProcessPoolExecutor() as executor:

            for index in range(publisher_num):
                send_process = executor.submit(
                    fn=send_many,
                    msgs_num=msgs_num,
                    msg_size=msg_size,
                    tmp_dir=tmp_dir)
                processes.append(send_process)

            for process in processes:
                process.result()

            end = time.time()

            for index in range(subscriber_num):
                receive_process = executor.submit(
                    fn=receive,
                    msgs_num=msgs_num * publisher_num,
                    identifier='sub{}'.format(index),
                    tmp_dir=tmp_dir)

                processes.append(receive_process)

            for process in processes:
                process.result()

        total_time = end - start
        average_time_per_msg = total_time / (msgs_num * publisher_num)
        print("Send many: {}|{}|{}|{} \t| Total time: {} \t| "
              "Average time per message: {}".format(
                  msgs_num, msg_size, publisher_num, subscriber_num,
                  round(total_time, 2), round(average_time_per_msg, 4)))

    return total_time, average_time_per_msg


def benchmark_receive(msgs_num: int, msg_size: int, publisher_num: int,
                      subscriber_num: int) -> Tuple[float, float]:
    """
    Run benchmark for receive with given configurations.

    :param msgs_num: number of messages
    :param msg_size: size of messages (in bytes)
    :param publisher_num: number of publishers
    :param subscriber_num: number of subscribers
    :return: Total runtime + average time per message
    """
    # pylint: disable=too-many-locals
    with temppathlib.TemporaryDirectory() as tmp_dir:
        env = persipubsub.environment.Environment(path=tmp_dir.path)
        sub_set = {"sub{}".format(index) for index in range(subscriber_num)}
        _ = setup(env=env, sub_set=sub_set)

        processes = []  # type: List[Any]

        with concurrent.futures.ProcessPoolExecutor() as executor:

            for index in range(publisher_num):
                send_process = executor.submit(
                    fn=send_many,
                    msgs_num=msgs_num,
                    msg_size=msg_size,
                    tmp_dir=tmp_dir)
                processes.append(send_process)

            for process in processes:
                process.result()

            start = time.time()
            for index in range(subscriber_num):
                receive_process = executor.submit(
                    fn=receive,
                    msgs_num=msgs_num * publisher_num,
                    identifier='sub{}'.format(index),
                    tmp_dir=tmp_dir)

                processes.append(receive_process)

            for process in processes:
                process.result()

        end = time.time()

        total_time = end - start
        average_time_per_msg = total_time / (msgs_num * publisher_num)
        print("Receive: {}|{}|{}|{} \t| Total time: {} \t| "
              "Average time per message: {}".format(
                  msgs_num, msg_size, publisher_num, subscriber_num,
                  round(total_time, 2), round(average_time_per_msg, 4)))

    return total_time, average_time_per_msg


def benchmark_live(msgs_num: int, msg_size: int, publisher_num: int,
                   subscriber_num: int) -> Tuple[float, float]:
    """
    Run benchmark for live with given configurations.

    :param msgs_num: number of messages
    :param msg_size: size of messages (in bytes)
    :param publisher_num: number of publishers
    :param subscriber_num: number of subscribers
    :return: Total runtime + average time per message
    """
    # pylint: disable=too-many-locals
    with temppathlib.TemporaryDirectory() as tmp_dir:
        env = persipubsub.environment.Environment(path=tmp_dir.path)
        sub_set = {"sub{}".format(index) for index in range(subscriber_num)}
        _ = setup(env=env, sub_set=sub_set)

        processes = []  # type: List[Any]

        start = time.time()
        with concurrent.futures.ProcessPoolExecutor() as executor:

            for index in range(publisher_num):
                send_process = executor.submit(
                    fn=send,
                    msgs_num=msgs_num,
                    msg_size=msg_size,
                    tmp_dir=tmp_dir)
                processes.append(send_process)
            for index in range(subscriber_num):
                receive_process = executor.submit(
                    fn=receive,
                    msgs_num=msgs_num * publisher_num,
                    identifier='sub{}'.format(index),
                    tmp_dir=tmp_dir)

                processes.append(receive_process)

        for process in processes:
            process.result()

        end = time.time()

        total_time = end - start
        average_time_per_msg = total_time / (msgs_num * publisher_num)
        print("Live: {}|{}|{}|{} \t| Total time: {} \t| "
              "Average time per message: {}".format(
                  msgs_num, msg_size, publisher_num, subscriber_num,
                  round(total_time, 2), round(average_time_per_msg, 4)))

    return total_time, average_time_per_msg


if __name__ == '__main__':
    run()
