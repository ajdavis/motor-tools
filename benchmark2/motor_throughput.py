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
import time

from tornado.ioloop import IOLoop
import motor
import pymongo

import benchmark2_common


def connect():
    loop = IOLoop.instance()
    c = motor.MotorClient()
    c.open(callback=lambda result, error: loop.stop())
    loop.start()
    return c




if __name__ == '__main__':
    args = benchmark2_common.parse_args()
    sync_client = pymongo.MongoClient()
    sync_client.drop_database('test')
    sync_client.test.test.insert({})

    c = connect()
    fn = c.test.test.find_one
    success, load, throughput = benchmark2_common.async_trial(fn, args.load, 5, 2)
    print 'success', success, 'load', load, 'throughput', throughput
