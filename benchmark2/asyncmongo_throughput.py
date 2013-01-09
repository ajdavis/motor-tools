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

import logging
logging.getLogger().setLevel(1000) # silence everything

import asyncmongo
import pymongo

import benchmark2_common


def connect():
    db = asyncmongo.Client(pool_id='mydb', host='127.0.0.1', port=27017, maxcached=6000, maxconnections=6000, dbname='test')
    return db


db = connect()


# This is what we're benchmarking
def fn(callback):
    db.test.find_one({}, callback=callback)


if __name__ == '__main__':
    benchmark2_common.main(fn, True)
