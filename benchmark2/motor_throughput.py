# Copyright 2013 10gen, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""MongoDB benchmarking suite."""

from functools import partial
import logging
logging.getLogger().setLevel(1000) # silence everything

from tornado.ioloop import IOLoop
import toro # from http://pypi.python.org/pypi/toro/
import motor
import pymongo

import benchmark2_common


def connect():
    loop = IOLoop.instance()
    c = motor.MotorClient()
    c.open(callback=lambda result, error: loop.stop())
    loop.start()
    assert c.connected
    return c


c = connect()
collection = c.test.test


semaphore = toro.Semaphore(10)


def _post_fn(callback, result, error):
    semaphore.release()
    callback(result, error)


# This is what we're benchmarking
def _inner_fn(callback, semaphore_result):
    collection.find_one(callback=partial(_post_fn, callback))


def fn(callback):
#    semaphore.acquire(callback=partial(_inner_fn, callback))
    _inner_fn(callback, None)


if __name__ == '__main__':
    benchmark2_common.main(fn, True)
