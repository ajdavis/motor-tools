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
    try:
        output = check_output(['python2.7', command, str(load)])
        match = re.search(pat, output.strip(), re.M)
        assert match, output.strip()
        return match.group('load'), match.group('throughput')
    except CalledProcessError:
        # Process threw exception
        return 0, 0


def printit(stats):
    print 'desired_load\tload\tthroughput'
    for desired_load, load, throughput in stats:
        print '\t'.join(['%.3f' % n for n in desired_load, load, throughput])


def main(command):
    desired_load = 1000
    stats = []
    while True:
        mongod = start_mongod()
        load, throughput = runit(command, desired_load)
        stats.append((desired_load, float(load), float(throughput)))
        stop_mongod(mongod)
        print desired_load, load, throughput

        # If we didn't achieve 80% of desired load or 50% throughput, break
        if float(load) / desired_load < .8 or float(throughput) / float(load) < .5:
            break

        if desired_load > 100000:
            # Run out of patience
            break

        desired_load += 1000

    printit(stats)


if __name__ == '__main__':
    check_ulimit()
    main(sys.argv[1])
