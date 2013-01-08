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
from functools import partial
import time

from tornado import gen
from tornado.ioloop import IOLoop
import motor

import benchmark2_common


def connect():
    loop = IOLoop.instance()
    c = motor.MotorClient()
    c.open(callback=lambda result, error: loop.stop())
    loop.start()
    return c


def trial(c, load, duration, warmup, maxqlen):
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
                print error
                loop.stop()
                st.success = False
            else:
                st.qlen -= 1
                if warmup < (time.time() - start) < warmup + duration:
                    st.ncompleted += 1

        def log():
            print 'so far', now - start, 'seconds_remaining', round(seconds_remaining, 2), 'nexpected', nexpected, 'n_to_go', n_to_go, 'interval %.4f' % interval, 'qlen', st.qlen, 'nstarted', st.nstarted, 'ncompleted', st.ncompleted

        now = time.time()
        while (now - start) < total_duration:
            seconds_so_far = now - start
            seconds_remaining = total_duration - seconds_so_far
            nexpected = seconds_so_far * load
            n_to_go = load * total_duration - st.nstarted
            if n_to_go > 0:
                interval = seconds_remaining / n_to_go
#                interval = benchmark2_common.poisson_interval(multiplier / float(load))

                if interval > .0001:
                    yield gen.Task(loop.add_timeout, now + interval)
                else:
                    yield gen.Task(loop.add_callback)

                st.qlen += 1
                st.nstarted += 1
                if (now - start) > warmup:
                    st.nstarted_after_warmup += 1
                loop.add_callback(partial(found_one, None, None))
                #c.test.test.find_one(callback=partial(found_one, should_record))

                if now - last_logged > 1:
                    log()
                    last_logged = now
            else:
                # n_to_go isn't greater than zero; wait to finish
                print 'waiting', start + warmup + duration - now, 'to finish'
                yield gen.Task(loop.add_timeout, start + warmup + duration)

            now = time.time()

        log()
        callback()

    _trial(loop.stop)
    loop.start()
    return st.success, st.nstarted_after_warmup / float(duration), st.ncompleted / float(duration)


if __name__ == '__main__':
    args = benchmark2_common.parse_args()
    c = connect()
    success, load, throughput = trial(c, args.load, 5, 2, 1000)
    print 'success', success, 'load', load, 'throughput', throughput
