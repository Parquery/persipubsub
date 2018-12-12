persipubsub
===========

*catch phrase in work*


Why not ...
-----------

persist-queue
^^^^^^^^^^^^^
* `protobuf <https://github.com/peter-wangxu/persist-queue/>`_ messages can't
  be send except if you deserialize it and serialize it with pickle.
* Offers not all functionality expected from a queue. Has put and get function
  which are basically only push and pop. So front/peak functionality is missing.
  So neither can multiple subscriber be implemented nor can't be guaranteed that
  no data is lost when a thread fails.

Kafka
^^^^^
* For our needs as local system with IPC it's an unnecessary overhead.
* Integration of Kafka written in Scala and Java in our C++/Python/Go codebase
  not that simple.
* Hard to setup and maintain `Kafka <https://kafka.apache.org/>`_.
* broker system eventually a bottleneck.

RabbitMQ
^^^^^^^^
* For our needs as local system with IPC it's an unnecessary overhead.
* broker system eventually a bottleneck.
* `RabbitMQ <https://www.rabbitmq.com//>`_ is less scalable than Kafka, but is
  supported officially by more languages.

zeromq persistence pattern
^^^^^^^^^^^^^^^^^^^^^^^^^^
* Only persistence pattern of `zeromq <http://zeromq.org//>`_ is
  `Titanic <https://rfc.zeromq.org/spec:9/TSP//>`_ which is also a broker
  system. This takes away the purpose and advantage of zeromq to be a
  lightweight library which requires no broker.
