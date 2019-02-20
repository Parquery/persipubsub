persipubsub
===========

.. image:: https://api.travis-ci.com/Parquery/persipubsub.svg?branch=master
    :target: https://api.travis-ci.com/Parquery/persipubsub.svg?branch=master
    :alt: Build Status

.. image:: https://coveralls.io/repos/github/Parquery/persipubsub/badge.svg?branch=master
    :target: https://coveralls.io/github/Parquery/persipubsub?branch=master
    :alt: Coverage

.. image:: https://readthedocs.org/projects/persipubsub/badge/?version=latest
    :target: https://persipubsub.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status

.. image:: https://badge.fury.io/py/persipubsub.svg
    :target: https://badge.fury.io/py/persipubsub
    :alt: PyPI - version

.. image:: https://img.shields.io/pypi/pyversions/persipubsub.svg
    :alt: PyPI - Python Version

.. image:: https://badges.frapsoft.com/os/mit/mit.png?v=103
    :target: https://opensource.org/licenses/mit-license.php
    :alt: MIT License

``persipubsub`` implements a persistent, thread-safe and process-safe queue for
inter-process communication, based on `lmdb <http://www.lmdb.tech/doc/>`_.

Primarily, we used `zeromq <http://zeromq.org//>`_ for inter-process
communication with a slight improvement through `persizmq
<https://github.com/Parquery/persizmq>`_. This still did not fulfill the level
of persistence we wanted.

Our motivation was to replace our previous library with a one which is
similarly easy to setup and to use. Additionally, it should make it possible to
send `protobuf <https://developers.google.com/protocol-buffers/>`_ messages
(bytes) thread-safely and process-safely from many publishers to many
subscribers.

Besides basic publisher and subscriber classes the library offers control
methods for easy deployment from a config JSON file and maintenance in case
needed.

Related projects
================

persist-queue
-------------

* The library offers not all functionality expected from a queue. It has put
  and get function which are basically only push and pop. Therefore ``front``
  functionality is missing. In consequence neither can the queue have multiple
  subscribers nor can be guaranteed that no data is lost when a thread fails.
* All messages in queues are serialized by ``pickle`` which was for us the main
  reason not to use this library.

Kafka
-----

* Hence we only need Inter Process Communication, the TCP ability of `Kafka
  <https://kafka.apache.org/>`_ is an unnecessary overhead.
* Integration of ``Kafka`` written in Scala and Java in our C++/Python/Go
  codebase is challenging.
* Hard to setup and maintain ``Kafka``.
* broker system eventually a bottleneck.

RabbitMQ
--------

* Hence we only need Inter Process Communication, the TCP ability of `RabbitMQ
  <https://www.rabbitmq.com//>`_ is an unnecessary overhead.
* broker system eventually a bottleneck.
* ``RabbitMQ`` is less scalable than ``Kafka``, but
  is supported officially by more languages.

zeromq persistence pattern
--------------------------

* `Titanic <https://rfc.zeromq.org/spec:9/TSP//>`_ is the only persistence
  pattern of `zeromq <http://zeromq.org//>`_ which is also a broker system.
  This takes away the purpose and advantage of ``zeromq`` to be a
  lightweight library which requires no broker.

Usage
=====

The usage of the library consists of two steps: deployment and runtime

Python
======

Environment
-----------

To improve the accessibility of the library, an environment class lets you
create and initialize any ``persipubsub`` component which you need in
deployment or runtime step.

.. warning::

  Only one environment of each queue per process allowed and it's forbidden to
  fork environment or any child components to multiple processes.
  In that case, persipubsub is multi-threading and multi-process safe.
  If multiple environments of the same queue are active on the same process,
  or environment is forked to multiple processes the lock is broken and
  correctness can't be guaranteed.

Initialize environment
^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    import persipubsub.environment

    env = persipubsub.environment.new_environment(path="/home/user/queue/")

Deployment
----------

In the deployment stage the library sets up the queue structure with the control.

Control
^^^^^^^

A control unit to initialize and maintain queues.

.. note::

    The high water marks are limits for the queue. The message is deleted in
    case that it reaches the timeout. In the other case of an overflow one
    of two strategies is used to prune half of the queue. The choice is between
    prune_first, which deletes the oldest messages, and prune_last, which
    deletes the latest messages.

Initialize queue
""""""""""""""""

.. code-block:: python

    import persipubsub.environment
    import persipubsub.queue

    env = persipubsub.environment.new_environment(path="/home/user/new-queue/")

    # Initialize a queue with default values.
    control = env.new_control()
    # Or define all optional parameters of the queue.
    hwm = persipubsub.queue._HighWaterMark()
    strategy = persipubsub.queue._Strategy.prune_first
    control = env.new_control(subscriber_ids=["sub1", "sub2"],
                              high_watermark=hwm,
                              strategy=strategy)

Prune all dangling messages
"""""""""""""""""""""""""""

.. code-block:: python

    import persipubsub.environment

    env = persipubsub.environment.new_environment(
        path="/home/user/queue-with-dangling-messages/")
    control = env.new_control()

    control.prune_dangling_messages()

Clear all messages
""""""""""""""""""

.. code-block:: python

    import persipubsub.environment

    env = persipubsub.environment.new_environment(
        path="/home/user/queue-with-subscribers-and-messages/")
    control = env.new_control()

    control.clear_all_subscribers()


Runtime
-------

During runtime only publisher and subscriber are needed.

.. note::

    Control can be optionally be used for pruning although the queues prune
    itself on a regular basis when high water mark is reached. The high water
    mark includes a timeout, maximum number of messages and the maximum bytes
    size of the queue.

Publisher
^^^^^^^^^

Initialization
""""""""""""""

Assuming that all queues were initialized during deployment the publisher can
be initialized as following.

.. code-block:: python

    import persipubsub.environment

    env = persipubsub.environment.new_environment(path="/home/user/queue/")

    pub = env.new_publisher()

Send a message
""""""""""""""

.. code-block:: python

    msg = "Hello there!".encode('utf-8')
    pub.send(msg=msg)

    # Subscribers have now a message in the queue.

Send many messages at once
""""""""""""""""""""""""""

.. code-block:: python

    msgs = ["What's up?".encode('utf-8'),
            "Do you like the README?".encode('utf-8')]
    pub.send_many(msgs=msgs)

    # Both messages are now available for the subscribers. Note that the order
    # of the messages are not necessarily kept.

Subscriber
^^^^^^^^^^

Initialization
""""""""""""""

Assuming that all queues were initialized during deployment the subscriber can
be initialized as following.

.. code-block:: python

    import persipubsub.environment

    env = persipubsub.environment.new_environment(path="/home/user/queue/")

    sub = env.new_subscriber(identifier="sub")

Receive a message
"""""""""""""""""

.. code-block:: python

    # One message in queue
    with sub.receive() as msg:
        # do something with the message
        print(msg)  # b'Hello there!'

    # This subscriber's queue is now empty

Catch up with latest message
""""""""""""""""""""""""""""

Can be used in the case when a particular subscriber cares only about the very
last message. The messages are not popped for other subscribers.

.. note::
    If you want to store only the latest message for all subscribers, then use
    high water mark max_msgs_num = 1.


.. code-block:: python

    # Many outdated messages in queue

    with sub.receive_to_top() as msg:
        # do something with the latest message

    # This subscriber's queue is now empty.

Documentation
=============

The documentation is available on `readthedocs
<https://persipubsub.readthedocs.io/en/latest/>`_.

Installation
============

* Install persipubsub with pip:

.. code-block:: bash

    pip3 install persipubsub

Development
===========

* Check out the repository.

* In the repository root, create the virtual environment:

.. code-block:: bash

    python3 -m venv venv3

* Activate the virtual environment:

.. code-block:: bash

    source venv3/bin/activate

* Install the development dependencies:

.. code-block:: bash

    pip3 install -e .[dev]

We use tox for testing and packaging the distribution. Assuming that the virtual
environment has been activated and the development dependencies have been
installed, run:

.. code-block:: bash

    tox

Pre-commit Checks
-----------------

We provide a set of pre-commit checks that lint and check code for formatting.

Namely, we use:

* `yapf <https://github.com/google/yapf>`_ to check the formatting.
* The style of the docstrings is checked with `pydocstyle <https://github.com/PyCQA/pydocstyle>`_.
* Static type analysis is performed with `mypy <http://mypy-lang.org/>`_.
* `isort <https://github.com/timothycrosley/isort>`_ to sort your imports for you.
* Various linter checks are done with `pylint <https://www.pylint.org/>`_.
* Doctests are executed using the Python `doctest module <https://docs.python.org/3.5/library/doctest.html>`_.
* `pyicontract-lint <https://github.com/Parquery/pyicontract-lint/>`_ lints contracts
  in Python code defined with `icontract library <https://github.com/Parquery/icontract/>`_.
* `twine <https://pypi.org/project/twine/>`_ to check the README for invalid markup
  which prevents it from rendering correctly on PyPI.

Run the pre-commit checks locally from an activated virtual environment with
development dependencies:

.. code-block:: bash

    ./precommit.py

* The pre-commit script can also automatically format the code:

.. code-block:: bash

    ./precommit.py  --overwrite

Versioning
==========

We follow `Semantic Versioning <http://semver.org/spec/v1.0.0.html>`_.
The version X.Y.Z indicates:

* X is the major version (backward-incompatible),
* Y is the minor version (backward-compatible), and
* Z is the patch version (backward-compatible bug fix).