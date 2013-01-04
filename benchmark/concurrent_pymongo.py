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

from pymongo import mongo_client
import threading


def main(total, concurrency):
    c = mongo_client.MongoClient(socketTimeoutMS=60*1000, auto_start_request=False)
    c.drop_database("benchmark")
    db = c.benchmark

    # Roughly 1kb documents
    db.benchmark.insert([
        {'_id': i, 's': 'a' * 1000} for i in range(100)
    ], safe=True)

    start_time = time.time()

    def find():
        for i in range(total / concurrency):
            assert len(list(db.benchmark.find())) == 100

    threads = [threading.Thread(target=find) for i in range(concurrency)]
    for t in threads:
        t.start()

    for t in threads:
        t.join()

    total_finds = (total / concurrency) * concurrency
    elapsed = time.time() - start_time
    print '%s %s %.2f %.2f' % (
        concurrency,
        total_finds,
        elapsed,
        float(total_finds) / elapsed
    )


if __name__ == "__main__":
    main(int(sys.argv[1]), int(sys.argv[2]))
