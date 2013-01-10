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
import os

from subprocess import check_output, Popen, PIPE, CalledProcessError
from shutil import rmtree
import re
import sys
import time


def check_ulimit():
    assert 10000 == int(check_output(['ulimit', '-n']).strip())


def start_mongod():
    if os.path.exists('data'):
        rmtree('data')

    os.mkdir('data')
    return Popen([
        '/usr/local/mongo/bin/mongod',
        '--logpath', 'data/mongo.log',
        '--dbpath', 'data',
        '--smallfiles', '--nojournal', '--noprealloc'],
        stdout=PIPE)


def stop_mongod(mongod):
    # mongod is a Popen object
    mongod.kill()


pat = r'^success (True|False) load (?P<load>\d+(\.\d+)?) throughput (?P<throughput>\d+(\.\d+)?)$'


def runit(command, load):
    mongod = start_mongod()
    time.sleep(10)
    try:
        output = check_output(['python2.7', command, str(load)])
        match = re.search(pat, output.strip(), re.M)
        assert match, output.strip()
        return match.group('load'), match.group('throughput')
    except CalledProcessError:
        # Process threw exception
        return 0, 0
    finally:
        stop_mongod(mongod)


def printit(stats):
    print 'desired_load\tload\tthroughput'
    for desired_load, load, throughput in stats:
        print '\t'.join(['%.3f' % n for n in desired_load, load, throughput])


def parse_args():
    parser = argparse.ArgumentParser(description=
"""Find a function's highest throughput, either by subjecting it to ever-higher
loads beginning at 1000 requests / sec, or by doing a binary search for the
highest load it can sustain.""")

    parser.add_argument(
        '--binary-search', '-b', dest='bsearch', action='store_true',
        help="Binary-search for highest throughput")

    parser.add_argument(
        'script', metavar="SCRIPT", help="The script to run")

    return parser.parse_args()


def main(args):
    if args.bsearch:
        max_throughput = 0

        # Search between 1000 and 10,000
        bottom, top = 1000, 10 * 1000
        while True:
            desired_load = (bottom + top) / 2
            load, throughput = runit(args.script, desired_load)
            max_throughput = max(throughput, max_throughput)
            print desired_load, load, throughput

            if (top - bottom) < 100:
                # Close enough
                print "I think", throughput, "is roughly the max throughput"
                break
            elif (float(load) / desired_load < .8
                or float(throughput) / float(load) < .9):
                # If we didn't achieve 80% of desired load or 90% throughput,
                # turn it down
                top = desired_load
            else:
                # Turn it up
                bottom = desired_load

            time.sleep(10) # breathe

    else:
        stats = []
        desired_load = 1000
        while True:
            load, throughput = runit(args.script, desired_load)
            stats.append((desired_load, float(load), float(throughput)))
            print desired_load, load, throughput

            # If we didn't achieve 80% of desired load or 50% throughput, break
            if float(load) / desired_load < .8 or float(throughput) / float(load) < .5:
                break

            if desired_load > 100000:
                # Run out of patience
                break

            desired_load += 1000
            time.sleep(1) # breathe

        printit(stats)


if __name__ == '__main__':
    check_ulimit()
    main(parse_args())
