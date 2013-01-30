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

import pymongo
import benchmark2_common

def connect():
    return pymongo.MongoClient()

c = connect()
collection = c.test.test


def fn():
    collection.find_one()


def log(sofar, c, st, seconds_remaining, nexpected, pool):
    try:
        print 'so far', sofar, 'seconds_remaining', round(seconds_remaining, 2), 'nexpected', nexpected, 'qlen', st.qlen, 'nstarted', st.nstarted, 'ncompleted', st.ncompleted, 'pool socks', len(c._MongoClient__pool.sockets), pool
    except Exception, e:
        print e


if __name__ == '__main__':
    benchmark2_common.main(log, c, fn, False)
