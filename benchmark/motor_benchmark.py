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

"""Motor benchmarking suite."""

import sys

import motor

sys.path[0:0] = [""]

# Import these names so they're available to main()
from async_common import insert, insert_batch, find_one, run
from benchmark_common import per_trial, main


def find(callback, asyncdb, collection, x):
    i = [per_trial]
    def inner_find(results, error):
        if error:
            raise error

        if i[0] == 0:
            # Complete
            callback()
        else:
            i[0] -= 1
            asyncdb[collection].find({"x": x}).to_list(callback=inner_find)

    inner_find(None, None)


if __name__ == "__main__":
    trial_db = motor.MotorClient().open_sync().benchmark
    main(globals())
