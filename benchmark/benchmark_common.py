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

"""MongoDB benchmarking suite. Common functions used by
   pymongo_benchmark.py, asyncmongo_benchmark.py, motor_benchmark.py, and
   motor_benchmark_gen.py. Helps ensure they're all benchmarking the same
   operations.
"""

from pymongo import connection, ASCENDING
import time
import sys


trials = 2
per_trial = 50
batch_size = 25
small = {}

medium = {"integer": 5,
          "number": 5.05,
          "boolean": False,
          "array": ["test", "benchmark"]
}

large = {
    'a': 'asdf asdf asdf asdfasdfasdf adsf asdf adsf asdf asdf asdf asdf asdf',
    'b': 2 ** 60,
    'c': ['asdf'] * 1000,
    'medium': medium,
    'd': [{'hello':'goodbye'} for i in range(100)],
}


def get_db():
    c = connection.Connection(network_timeout=60)
    c.drop_database("benchmark")
    return c.benchmark


def timed(run, db, trial_db, name, function, args=[], setup=None):
    """Run trial 'trials' times, and each time do it 'per_trials' times. Choose
       best time"""

    # print name first so we know what's happening if it takes a long time
    sys.stdout.write('%-40s' % name)
    sys.stdout.flush()

    times = []
    for _ in range(trials):
        if setup:
            setup(db, *args)
        start = time.time()
        run(function, *([trial_db] + args))
        times.append(time.time() - start)
    best_time = min(times)

    print '%20.2f%20.2f' % (
        1000 * best_time, 1000 * (best_time / per_trial))
    return best_time


def setup_insert(db, collection, object):
    db.drop_collection(collection)


def main(config):
    start_time = time.time()
    db = get_db()
    trial_db = config['trial_db']

    # Get some functions specific to benchmarking PyMongo, AsyncMongo, or Motor
    run = config['run']
    insert = config['insert']
    insert_batch = config['insert_batch']
    find_one = config['find_one']
    find = config['find']

    print '%-40s%20s%20s' % (' ', 'ms per trial', 'ms per run')

    timed(run, db, trial_db, "insert (small, no index)", insert,
        ['small_none', small], setup_insert)
    timed(run, db, trial_db, "insert (medium, no index)", insert,
        ['medium_none', medium], setup_insert)
    timed(run, db, trial_db, "insert (large, no index)", insert,
        ['large_none', large], setup_insert)

    db.small_index.create_index("x", ASCENDING)
    timed(run, db, trial_db, "insert (small, indexed)", insert, ['small_index', small])
    db.medium_index.create_index("x", ASCENDING)
    timed(run, db, trial_db, "insert (medium, indexed)", insert, ['medium_index', medium])
    db.large_index.create_index("x", ASCENDING)
    timed(run, db, trial_db, "insert (large, indexed)", insert, ['large_index', large])

    timed(run, db, trial_db, "batch insert (small, no index)", insert_batch,
        ['small_bulk', small], setup_insert)
    timed(run, db, trial_db, "batch insert (medium, no index)", insert_batch,
        ['medium_bulk', medium], setup_insert)
    timed(run, db, trial_db, "batch insert (large, no index)", insert_batch,
        ['large_bulk', large], setup_insert)

    timed(run, db, trial_db, "find_one (small, no index)", find_one,
        ['small_none', per_trial / 2])
    timed(run, db, trial_db, "find_one (medium, no index)", find_one,
        ['medium_none', per_trial / 2])
    timed(run, db, trial_db, "find_one (large, no index)", find_one,
        ['large_none', per_trial / 2])

    timed(run, db, trial_db, "find_one (small, indexed)", find_one,
        ['small_index', per_trial / 2])
    timed(run, db, trial_db, "find_one (medium, indexed)", find_one,
        ['medium_index', per_trial / 2])
    timed(run, db, trial_db, "find_one (large, indexed)", find_one,
        ['large_index', per_trial / 2])

    timed(run, db, trial_db, "find (small, no index)", find, ['small_none', per_trial / 2])
    timed(run, db, trial_db, "find (medium, no index)", find, ['medium_none', per_trial / 2])
    timed(run, db, trial_db, "find (large, no index)", find, ['large_none', per_trial / 2])

    timed(run, db, trial_db, "find (small, indexed)", find, ['small_index', per_trial / 2])
    timed(run, db, trial_db, "find (medium, indexed)", find, ['medium_index', per_trial / 2])
    timed(run, db, trial_db, "find (large, indexed)", find, ['large_index', per_trial / 2])

    timed(run, db, trial_db, "find range (small, indexed)", find,
        ['small_index',
                {"$gt": per_trial / 2, "$lt": per_trial / 2 + batch_size}])
    timed(run, db, trial_db, "find range (medium, indexed)", find,
        ['medium_index',
                {"$gt": per_trial / 2, "$lt": per_trial / 2 + batch_size}])
    timed(run, db, trial_db, "find range (large, indexed)", find,
        ['large_index',
                {"$gt": per_trial / 2, "$lt": per_trial / 2 + batch_size / 4}])

    print 'total elapsed %.2f seconds' % (time.time() - start_time)