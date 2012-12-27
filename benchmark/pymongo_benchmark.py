# Copyright 2009-2012 10gen, Inc.
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

"""Motor benchmarking suite. Reference benchmark using 10gen's official
   blocking driver, PyMongo.
"""

import sys

sys.path[0:0] = [""]

from benchmark_common import per_trial, batch_size, main

from pymongo import mongo_client
from pymongo import ASCENDING


def insert(db, collection, object):
    """Unsafe inserts"""
    for i in range(per_trial):
        to_insert = object.copy()
        to_insert["x"] = i
        db[collection].insert(to_insert)


def insert_batch(db, collection, object):
    for i in range(per_trial / batch_size):
        db[collection].insert([object.copy() for _ in xrange(batch_size)])


def find_one(db, collection, x):
    for _ in range(per_trial):
        db[collection].find_one({"x": x})


def find(db, collection, x):
    for _ in range(per_trial):
        list(db[collection].find({"x": x}))


def run(function, *args):
    function(*args)


if __name__ == "__main__":
    trial_db = mongo_client.MongoClient().benchmark
    main(globals())
