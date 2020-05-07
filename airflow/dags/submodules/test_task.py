import unittest

from unittest import mock
from tempfile import TemporaryDirectory
from submodules.task import requires

class PythonIdempatomicFileOperatorTest_Requires(unittest.TestCase):

    def req_test(self, req_path, expected_result):
        """
        NOT A TEST: Generic test function which can be used to implement multiple test with
        similar structure

        :param req_path: The fake-path that xcom_pull will return
        :param expected_result: The dictionary that should be returned for fake task
        """


        requirement_path = req_path

        # need to mock TaskInstance since, according to multiple SO posts, Xcom data does not
        # persist in test mode
        ti = mock.Mock()
        ti.xcom_pull.return_value = requirement_path

        # fake Airflow context kwargs
        kwargs = {'ti': ti}

        # requires takes task_id and **kwargs as arguments
        result = requires('test', **kwargs)

        self.assertEqual(result, expected_result)

    def test_requires_file(self):
        """
        Ensures that a Operator that returns path to a *single file* returns a dictionary with a
        single key
        """
        req_path = 'foo/bar/test.txt'
        expected_result = {'test': req_path}
        self.req_test(req_path, expected_result)


    def test_requires_dir(self):
        """
        Ensures that an Operator that returns a *directory* output_path returns a dict keyed on
        names of files in that directory
        """
        with TemporaryDirectory() as tempdir:
            path_1 = f'{tempdir}/file_1.txt'
            path_2 = f'{tempdir}/file_2.txt'
            open(path_1, 'w')
            open(path_2, 'w')
            expected_result = {'file_1': path_1, 'file_2': path_2}
            self.req_test(tempdir + '/', expected_result)