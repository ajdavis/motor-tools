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

"""MongoDB benchmarking suite."""

import time
import sys

from tornado import gen
from tornado.ioloop import IOLoop

from pymongo import mongo_client
import motor


def main(total, concurrency, open_sync):
    sync_c = mongo_client.MongoClient(
        socketTimeoutMS=60*1000, auto_start_request=False)
    sync_c.drop_database("benchmark")

    # Roughly 1kb documents
    sync_c.benchmark.benchmark.insert([
        {'_id': i, 's': 'a' * 1000} for i in range(100)
    ], safe=True)

    loop = IOLoop.instance()
    if open_sync:
        c = motor.MotorClient().open_sync()
    else:
        c = motor.MotorClient()
        c.open(lambda a, b: loop.stop())
        loop.start()

    db = c.benchmark

    @gen.engine
    def find(callback):
        for i in range(total / concurrency):
            assert len((yield motor.Op(db.benchmark.find().to_list))) == 100

        callback()

    @gen.engine
    def start_tasks(callback):
        for i in range(concurrency):
            find(callback=(yield gen.Callback(i)))

        yield gen.WaitAll(range(concurrency))
        callback()

    start_time = time.time()
    start_tasks(callback=loop.stop)
    loop.start()
    elapsed = time.time() - start_time
    total_finds = (total / concurrency) * concurrency

    print '%s %s %s %.2f %.2f' % (
        'sync' if open_sync else 'async',
        concurrency,
        total_finds,
        elapsed,
        float(total_finds) / elapsed
    )


if __name__ == "__main__":
    main(int(sys.argv[1]), int(sys.argv[2]), sys.argv[3] == 'true')
