persipubsub
===========

.. image:: https://badges.frapsoft.com/os/mit/mit.png?v=103
    :target: https://opensource.org/licenses/mit-license.php
    :alt: MIT License

.. image:: https://badge.fury.io/py/persipubsub.svg
    :target: https://badge.fury.io/py/persipubsub
    :alt: PyPI - version

.. image:: https://img.shields.io/pypi/pyversions/persipubsub.svg
    :alt: PyPI - Python Version

.. image:: https://readthedocs.org/projects/persipubsub/badge/?version=latest
    :target: https://persipubsub.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status


``persipubsub`` implements a persistent, thread-safe and process-safe `lmdb
<http://www.lmdb.tech/doc/>`_-queue for inter-process communication.

Primarily, we used `zeromq <http://zeromq.org//>`_ for inter-process
communication with a slight improvement through `persizmq
<https://github.com/Parquery/persizmq>`_. This still didn't fulfill the level
of persistence we wanted.

Our motivation was to replace our previous library with a one which is
similarly easy to setup and to use. Additionally, it should make it possible to
send `protobuf <https://developers.google.com/protocol-buffers/>`_ messages
more precisely bytes persistently, thread-safely and process-safely from
many publishers to many subscribers.

Besides basic publisher and subscriber classes the library offers control
methods for easy deployment from a config JSON file and maintenance in case
needed.

Related projects
================

persist-queue
-------------

* Offers not all functionality expected from a queue. Has put and get function
  which are basically only push and pop. So ``front`` functionality is missing.
  So neither can multiple subscriber be implemented nor can't be guaranteed that
  no data is lost when a thread fails.
* All messages in queues are serialized by ``pickle`` which was for us a reason
  against using this library.

Kafka
-----
* For our needs as local system with IPC it's an unnecessary overhead.
* Integration of Kafka written in Scala and Java in our C++/Python/Go codebase
  not that simple.
* Hard to setup and maintain `Kafka <https://kafka.apache.org/>`_.
* broker system eventually a bottleneck.

RabbitMQ
--------
* For our needs as local system with IPC it's an unnecessary overhead.
* broker system eventually a bottleneck.
* `RabbitMQ <https://www.rabbitmq.com//>`_ is less scalable than Kafka, but is
  supported officially by more languages.

zeromq persistence pattern
--------------------------
* Only persistence pattern of `zeromq <http://zeromq.org//>`_ is
  `Titanic <https://rfc.zeromq.org/spec:9/TSP//>`_ which is also a broker
  system. This takes away the purpose and advantage of zeromq to be a
  lightweight library which requires no broker.

Usage
=====

The usage of the library can be differentiated into deployment and actual
runtime of your processes.

Deployment
----------

For deployment only a proper config JSON file is needed to setup the whole
queue structure.

config.json
^^^^^^^^^^^
Publishers, subscribers and queues need to be defined before runtime in the
config JSON file.

.. code-block:: json

    {
        "pub": {
            "out_queue": "/home/user/queues/queue",
            "subscribers": ["sub"]
        },
        "sub": {
            "in_queue": "/home/user/queues/queue"
        },
        "queues": {
            "/home/user/queues/queue": {
                "path": "/home/user/queues/queue",
                "max_reader_num": 1024,
                "max_db_num": 1024,
                "max_db_size_bytes": 34359738368,
                "subscribers": ["sub"],
                "high-water-mark": {
                    "MSG_TIMEOUT_SECS": 600,
                    "MAX_MSGS_NUM": 10000,
                    "HWM_LMDB_SIZE_BYTES": 1000000,
                    "strategy": "prune_first"
                }
            }
        }
    }

Control
^^^^^^^

Initialize all queues
"""""""""""""""""""""

.. code-block:: python

    import persipubsub.control

    persipubsub.control.initialize_all_dbs(config_pth="/home/user/config.json")

Prune all dangling messages
"""""""""""""""""""""""""""

.. code-block:: python

    import persipubsub.control

    persipubsub.control.prune_dangling_messages(
        config_pth="/home/user/config.json")

Clear all messages
""""""""""""""""""

.. code-block:: python

    import persipubsub.control

    persipubsub.control.clear_all_subs(config_pth="/home/user/config.json")


Runtime
-------
During runtime only publisher and subscriber are needed. Control can be
optionally be used for pruning although the queues prune itself on a regular
basis.

Publisher
^^^^^^^^^

Initialization
""""""""""""""

Assuming that all queues were initialized during deployment the publisher can
be initialized as following.

.. code-block:: python

    import persipubsub.publisher

    pub = persipubsub.publisher.Pub()
    pub.init(pub_id="pub", config_pth="/home/user/config.json")

Send a message
""""""""""""""

.. code-block:: python

    msg = "Hello there!".encode('utf-8)
    pub.send(msg=msg)

    # subscribers have now a message in the queue

Subscriber
^^^^^^^^^^

Initialization
""""""""""""""

Assuming that all queues were initialized during deployment the publisher can
be initialized as following.

.. code-block:: python

    import persipubsub.subscriber

    sub = persipubsub.subscriber.Sub()
    sub.init(sub_id="sub", config_pth="/home/user/config.json")

Receive a message
"""""""""""""""""

.. code-block:: python

    # one message in queue
    with sub.receive() as msg:
        # do something with the message
        print(msg)  # b'Hello there!'

    # sub queue is now empty

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