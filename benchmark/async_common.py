# Copyright 2012 10gen, Inc.
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

"""Motor benchmarking suite. These are common functions that take advantage
   of the similarity between Motor's and AsyncMongo's interfaces.
"""

from benchmark_common import per_trial, batch_size
from tornado import ioloop


def insert(callback, asyncdb, collection, object):
    """Asynchronous, SAFE inserts -- this is unfairly slow for AsyncMongo,
       since Motor and PyMongo do unsafe inserts for this test
    """
    i = [per_trial]
    def inner_insert(result, error):
        if error:
            raise error

        if i[0] == 0:
            # Complete
            callback()
        else:
            i[0] -= 1
            to_insert = object.copy()
            to_insert["x"] = i
            asyncdb[collection].insert(
                to_insert, callback=inner_insert)

    inner_insert(None, None)


def insert_batch(callback, asyncdb, collection, object):
    i = [int(per_trial / batch_size)]
    def inner_insert_batch(result, error):
        if error:
            raise error

        if i[0] == 0:
            # Complete
            callback()
        else:
            i[0] -= 1
            asyncdb[collection].insert(
                [object] * batch_size, callback=inner_insert_batch)

    inner_insert_batch(None, None)


def find_one(callback, asyncdb, collection, x):
    i = [per_trial]
    def inner_find_one(result, error):
        if error:
            raise error

        if i[0] == 0:
            # Complete
            callback()
        else:
            i[0] -= 1
            asyncdb[collection].find_one({"x": x}, callback=inner_find_one)

    inner_find_one(None, None)




def run(function, *args):
    loop = ioloop.IOLoop.instance()

    def callback():
        loop.stop()

    function(callback, *args)
    loop.start()
