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
from tornado import gen

#logging.getLogger().setLevel(1000) # silence everything

from tornado.ioloop import IOLoop
import motor
import bson
assert bson._use_c
import pymongo
assert pymongo.has_c()
import benchmark2_common


c = None
collection = None


def fn(callback):
    collection.find_one(callback=callback)


def log(sofar, c, st, seconds_remaining, nexpected):
    try:
        print 'so far', sofar, 'seconds_remaining', round(seconds_remaining, 2), 'nexpected', nexpected, 'qlen', st.qlen, 'nstarted', st.nstarted, 'ncompleted', st.ncompleted, 'pool socks', len(c.delegate._MongoClient__pool.sockets)
    except Exception, e:
        print e

if __name__ == '__main__':
    loop = IOLoop.instance()
    c = motor.MotorClient(host='127.0.0.1', max_pool_size=6000, connectTimeoutMS=None)
    c.open(callback=lambda result, error: loop.stop())
    loop.start()
    assert c.connected

    collection = c.test.test
    benchmark2_common.main(log, c, fn, True)
