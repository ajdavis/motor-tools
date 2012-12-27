motor-tools
===========

Tools for developing and testing Motor, an asynchronous Python driver for
Tornado and MongoDB.

http://emptysquare.net/motor

Benchmarking
------------

`benchmark/pymongo_benchmark.py`: Reference benchmark using synchronous PyMongo

`benchmark/asyncmongo_benchmark.py`: Reference benchmark using bit.ly's AsyncMongo

`benchmark/motor_benchmark.py`: Motor benchmark

`benchmark/motor_benchmark_gen.py`: Motor benchmark using Tornado's generator API

Testing
-------

`functional_test/test_motor_crosshost_copydb.py`: Start two instances of mongod
and test copying a database from one to the other with auth and Motor.
