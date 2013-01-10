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
import threading
import math
from Queue import Queue
import sys

from tornado import gen
from tornado.ioloop import IOLoop
import pymongo


def parse_args():
    parser = argparse.ArgumentParser(description='Benchmark a script')
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
            print 'so far', now - start, 'seconds_remaining', round(seconds_remaining, 2), 'nexpected', nexpected, 'qlen', st.qlen, 'nstarted', st.nstarted, 'ncompleted', st.ncompleted

        def bail(exc):
            print 'bail', exc
            st.success = False
#            loop.stop()

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


# Adapted from http://stackoverflow.com/questions/3033952/python-thread-pool-similar-to-the-multiprocessing-pool
class Worker(threading.Thread):
    """Thread executing tasks from a given tasks queue"""
    def __init__(self, pool):
        threading.Thread.__init__(self)
        self.pool = pool
        self.stopped = False
        self.setDaemon(True)
        self.go = threading.Event()
        self.start()

    def do(self, fn):
        self.fn = fn
        self.go.set()

    def run(self):
        while not self.stopped:
            self.go.wait()
            try:
                self.fn()
                self.pool.callback()
            except Exception, e:
                print e
                self.pool.exc_callback(e)
            finally:
                with self.pool.lock:
                    self.pool.working.remove(self)
                    # I don't care about the circular reference
                    self.pool.idle.add(self)

                self.go.clear()
                self.go.wait()


class ThreadPool:
    def __init__(self, callback, exc_callback):
        self.idle = set()
        self.working = set()
        self.callback = callback
        self.exc_callback = exc_callback
        self.lock = threading.Lock()

    def add_task(self, func):
        try:
            with self.lock:
                worker = self.idle.pop()
        except KeyError:
            worker = Worker(self)

        with self.lock:
            self.working.add(worker)

        worker.do(func)

    def cancel(self):
        with self.lock:
            for w in self.working:
                w.stopped = True


def sync_trial(fn, load, duration, warmup):
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
    start = time.time()
    last_logged = time.time()

    def found_one():
        st.qlen -= 1
        if warmup <= (time.time() - start) < warmup + duration:
            st.ncompleted += 1

    def log():
        return
        print 'so far', now - start, 'seconds_remaining', round(seconds_remaining, 2), 'nexpected', nexpected, 'qlen', st.qlen, 'nstarted', st.nstarted_after_warmup, 'ncompleted', st.ncompleted

    def bail(exc_info):
        print exc_info
        st.success = False
        pool.cancel()

    pool = ThreadPool(found_one, bail)
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
                pool.add_task(fn)
            except Exception, e:
                bail(e)

        if now - last_logged > 1:
            log()
            last_logged = now

        time.sleep(0.0001)

        now = time.time()

    log()
    return st.success, st.nstarted_after_warmup / float(duration), st.ncompleted / float(duration)


def main(fn, is_async):
    args = parse_args()
    sync_client = pymongo.MongoClient()
    sync_client.drop_database('test')
    sync_client.test.test.insert({})
    if is_async:
        success, load, throughput = async_trial(fn, args.load, 5, 2)
    else:
        success, load, throughput = sync_trial(fn, args.load, 5, 2)
    print 'success', success, 'load', load, 'throughput', throughput
