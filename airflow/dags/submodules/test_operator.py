import unittest

import os
from airflow import DAG
from airflow.models.taskinstance import TaskInstance
from unittest import mock
from datetime import datetime
from tempfile import TemporaryDirectory
from submodules.operator import PythonIdempatomicFileOperator
from submodules.task import requires


TEST_DAG_ID = 'test_my_custom_operator'

def mock_func(output):
    # function that allows me to flexibly mock a function and return whatever I want
    def wrapped(*args, **kwargs):
        return output
    return wrapped

class PythonIdempatomicFileOperatorTest_Idempotent(unittest.TestCase):
    def f(self, output_path):
        with open(output_path, 'a+') as fout:
            fout.write('test')

    def test_PyIdempaOp_idempotent(self):
        self.dag = DAG(TEST_DAG_ID, schedule_interval='@daily', default_args={'start_date' : datetime.now()})


        with TemporaryDirectory() as tempdir:
            output_path = f'{tempdir}/test_file.txt'

            self.assertFalse(os.path.exists(output_path)) # ensure doesn't already exist

            self.op = PythonIdempatomicFileOperator(
               dag=self.dag,
               task_id='test',
               output_pattern = output_path,
               python_callable = self.f,
            )
            self.ti = TaskInstance(task=self.op, execution_date=datetime.now())

            result = self.op.execute(self.ti.get_template_context())
            self.assertEqual(result, output_path)
            self.assertFalse(self.op.previously_completed)
            self.assertTrue(os.path.exists(output_path))

            with open(output_path, 'r') as fout:
                self.assertEqual(fout.read(), 'test')

            # now run task again
            result = self.op.execute(self.ti.get_template_context())
            self.assertEqual(result, output_path) # result will still give path
            self.assertTrue(self.op.previously_completed)

            # if function had run again, it would now be 'testtest'
            with open(output_path, 'r') as fout:
                self.assertEqual(fout.read(), 'test')

            # run function again to ensure 'testtest' is written to file upon second call
            self.f(output_path)
            with open(output_path, 'r') as fout:
                self.assertEqual(fout.read(), 'testtest')


class PythonIdempatomicFileOperatorTest_Atomic(unittest.TestCase):
    def g(self, output_path):
        with open(output_path, 'w') as fout:
            fout.write('test')
            raise ValueError('You cannot write that!')

    def test_PyIdempaOp_atomic(self):
        self.dag = DAG(TEST_DAG_ID, schedule_interval='@daily',
                       default_args={'start_date': datetime.now()})

        with TemporaryDirectory() as tempdir:
            output_path = f'{tempdir}/test.txt'

            self.assertFalse(os.path.exists(output_path))  # ensure doesn't already exist

            self.op = PythonIdempatomicFileOperator(
                dag=self.dag,
                task_id='test',
                output_pattern=output_path,
                python_callable=self.g,
            )
            self.ti = TaskInstance(task=self.op, execution_date=datetime.now())

        with self.assertRaises(ValueError): # ensure ValueError is triggered (since task ran)
            result = self.op.execute(self.ti.get_template_context())
            self.assertEqual(result, None) # make sure no path is returned

        self.assertFalse(os.path.exists(output_path)) # no partially written file


class PythonIdempatomicFileOperatorTest_Requires(unittest.TestCase):

    def req_test(self, req_path, expected_result):
        # need to mock TaskInstance since Xcom does not "work" in test mode
        requirement_path = req_path

        ti = mock.Mock()
        ti.xcom_pull.return_value = requirement_path
        kwargs = {'ti': ti}
        result = requires('test', **kwargs)
        self.assertEqual(result, expected_result)

    def test_req_file(self):
        req_path = 'foo/bar/test.txt'
        expected_result = {'test': req_path}
        self.req_test(req_path, expected_result)

    def test_requires_file(self):
        # need to mock TaskInstance since Xcom does not "work" in test mode
        requirement_path = 'foo/bar/test.txt'

        ti = mock.Mock()
        ti.xcom_pull.return_value = requirement_path
        kwargs = {'ti': ti}
        result = requires('test', **kwargs)
        self.assertEqual(result, {'test': requirement_path})

    def test_requires_dir(self):
        with TemporaryDirectory() as tempdir:
            path_1 = f'{tempdir}/file_1.txt'
            path_2 = f'{tempdir}/file_2.txt'
            open(path_1, 'w')
            open(path_2, 'w')
            expected_result = {'file_1': path_1, 'file_2': path_2}
            self.req_test(tempdir + '/', expected_result)