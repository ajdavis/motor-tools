import shutil
import socket
import subprocess
import unittest
import motor
import os
import time
from tornado import gen
from tornado.ioloop import IOLoop


class TestCopyDB(unittest.TestCase):
    def wait_for(self, proc, port_num):
        trys = 0
        while proc.poll() is None and trys < 160:
            trys += 1
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                try:
                    s.connect(('localhost', port_num))
                    return True
                except (IOError, socket.error):
                    time.sleep(0.25)
            finally:
                s.close()

        raise Exception("Failed to start mongod on port %s" % port_num)

    def test_crosshost_copydb(self):
        succeeded = [False]

        # Start two mongods
        procs = []
        dbpaths = []
        try:
            for port in (9000, 9001):
                dbpath = 'dbpath' + str(port)
                shutil.rmtree(dbpath, ignore_errors=True)
                os.mkdir(dbpath)
                cmd = [
                    'mongod', '--dbpath', dbpath, '--port', str(port), '--auth']
                proc = subprocess.Popen(
                    cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                self.wait_for(proc, port)
                procs.append(proc)
                dbpaths.append(dbpath)

            @gen.engine
            def f(callback):
                try:
                    m0 = yield motor.Op(motor.MotorClient(port=9000).open)
                    # Make a user
                    yield motor.Op(m0.admin.add_user, 'admin', 'passwd')
                    yield motor.Op(m0.admin.authenticate, 'admin', 'passwd')

                    # Create a database
                    yield motor.Op(m0.drop_database, 'test_source')
                    yield motor.Op(m0.test_source.add_user, 'foo', 'bar')
                    yield motor.Op(m0.test_source.test_collection.insert,
                        {'_id': 1})

                    # Copy the database
                    m1 = yield motor.Op(motor.MotorClient(port=9001).open)
                    yield motor.Op(m0.drop_database, 'test_destination')
                    yield motor.Op(m1.copy_database,
                        'test_source', 'test_destination',
                        from_host='localhost:9000',
                        username='foo', password='bar')

                    result = yield motor.Op(
                        m1.test_destination.test_collection.find().to_list)

                    self.assertEqual([{'_id': 1}], result)
                finally:
                    callback()

                # We didn't throw any exceptions
                succeeded[0] = True

            loop = IOLoop.instance()
            f(callback=loop.stop)
            loop.start()
        finally:
            errors = []
            for proc in procs:
                try:
                    proc.terminate()
                except Exception, e:
                    errors.append(e)

            for dbpath in dbpaths:
                shutil.rmtree(dbpath)

            if errors:
                raise errors[0]


        self.assertTrue(succeeded[0])


if __name__ == '__main__':
    unittest.main()
