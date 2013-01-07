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
args = benchmark2_common.parse_args()


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
    st.ncompleted = 0
    st.should_record = False

    multiplier = 100

    def start_recording():
        st.should_record = True

    def stop_recording():
        st.should_record = False

    loop = IOLoop.instance()

    # Start recording after warmup
    loop.add_timeout(time.time() + warmup, start_recording)

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
                if st.should_record:
                    st.ncompleted += 1

        now = time.time()
#        while st.qlen < maxqlen and (now - start) < (duration + warmup):
        while (now - start) < (duration + warmup):
#            c.test.test.find_one(callback=partial(found_one, should_record))
            for _ in range(multiplier):
                st.qlen += 1
                found_one(None, None)
#                loop.add_callback(partial(found_one, None, None))

            interval = multiplier / float(load)
#            interval = benchmark2_common.poisson_interval(multiplier / float(load))
#            yield gen.Task(loop.add_callback)
            yield gen.Task(loop.add_timeout, now + interval)
            if now - last_logged > 1:
                print 'should_record', st.should_record, 'interval %.2f' % interval, 'qlen', st.qlen, 'ncompleted', st.ncompleted
                last_logged = now

            now = time.time()

        print 'quitting with qlen', st.qlen
        callback()

    start = time.time()
    _trial(loop.stop)
    loop.start()
    return st.success, st.ncompleted / float(duration)


c = connect()
success, throughput = trial(c, args.load, 5, 2, 1000)
print 'success', success, 'throughput', throughput
