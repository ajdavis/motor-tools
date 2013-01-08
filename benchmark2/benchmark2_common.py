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
import time
import math
from tornado import gen
from tornado.ioloop import IOLoop


def parse_args():
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument(
        'load', type=int, help='Number of requests to begin per second')
    return parser.parse_args()


def async_trial(fn, load, duration, warmup):
    # An object so it can be modified from inner functions
    class State(object):
        pass

    st = State()
    st.success = True
    st.qlen = 0
    st.nstarted = 0
    st.nstarted_after_warmup = 0
    st.ncompleted = 0

    total_duration = duration + warmup
    loop = IOLoop.instance()
    start = time.time()

    @gen.engine
    def _trial(callback):
        last_logged = time.time()

        def found_one(result, error):
            if error:
                bail(error)
            else:
                st.qlen -= 1
                if warmup <= (time.time() - start) < warmup + duration:
                    st.ncompleted += 1

        def log():
            print 'so far', now - start, 'seconds_remaining', round(seconds_remaining, 2), 'nexpected', nexpected, 'qlen', st.qlen, 'nstarted', st.nstarted_after_warmup, 'ncompleted', st.ncompleted

        def bail(exc):
            print exc
            st.success = False
            loop.stop()

        now = time.time()
        while (now - start) < total_duration:
            seconds_so_far = now - start
            seconds_remaining = total_duration - seconds_so_far
            nexpected = seconds_so_far * load

            for _ in xrange(int(math.ceil(nexpected - st.nstarted))):
                st.qlen += 1
                st.nstarted += 1
                if (now - start) > warmup:
                    st.nstarted_after_warmup += 1
                try:
                    fn(callback=found_one)
                except Exception, e:
                    bail(e)

            if now - last_logged > 1:
                log()
                last_logged = now

            yield gen.Task(loop.add_timeout, now + 0.0001)

            now = time.time()

        log()
        callback()

    _trial(loop.stop)
    loop.start()
    return st.success, st.nstarted_after_warmup / float(duration), st.ncompleted / float(duration)
