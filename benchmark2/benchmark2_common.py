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

import argparse
import math
import random


def parse_args():
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument(
        'load', type=int, help='Number of requests to begin per second')
    return parser.parse_args()


def poisson_interval(mean_interarrival_time):
    # How long should we wait until beginning the next request, in order to
    # average (1 / mean_interarrival_time) requests per unit time?
    return (-math.log(1.0 - random.random()) * mean_interarrival_time)
