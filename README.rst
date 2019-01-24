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


*catch phrase in work*

What we wanted.
Motivation
Strengths

Related projects
================

persist-queue
-------------
* `protobuf <https://developers.google.com/protocol-buffers/>`_ messages can't
  be send with `persist-queue <https://github.com/peter-wangxu/persist-queue/>`_ except if you deserialize it and serialize it with pickle.
* Offers not all functionality expected from a queue. Has put and get function
  which are basically only push and pop. So front/peak functionality is missing.
  So neither can multiple subscriber be implemented nor can't be guaranteed that
  no data is lost when a thread fails.

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

*Intro text*

Deployment
----------

config.json
^^^^^^^^^^^

Control
^^^^^^^

Runtime
-------

Queue
^^^^^

Publisher
^^^^^^^^^

Subscriber
^^^^^^^^^^

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