import unittest

import os
from airflow import DAG
from airflow.models.taskinstance import TaskInstance
from unittest import mock
from datetime import datetime
from tempfile import TemporaryDirectory
from submodules.operator import PythonIdempatomicFileOperator
from submodules.task import requires


TEST_DAG_ID = "test_my_custom_operator"


class PythonIdempatomicFileOperatorTest_Idempotent(unittest.TestCase):
    def f(self, output_path):
        with open(output_path, "a+") as fout:
            fout.write("test")

    def test_PyIdempaOp_idempotent(self):
        self.dag = DAG(
            TEST_DAG_ID,
            schedule_interval="@daily",
            default_args={"start_date": datetime.now()},
        )

        with TemporaryDirectory() as tempdir:
            output_path = f"{tempdir}/test_file.txt"

            self.assertFalse(
                os.path.exists(output_path)
            )  # ensure doesn't already exist

            self.op = PythonIdempatomicFileOperator(
                dag=self.dag,
                task_id="test",
                output_pattern=output_path,
                python_callable=self.f,
            )
            self.ti = TaskInstance(task=self.op, execution_date=datetime.now())

            result = self.op.execute(self.ti.get_template_context())
            self.assertEqual(result, output_path)
            self.assertFalse(self.op.previously_completed)
            self.assertTrue(os.path.exists(output_path))

            with open(output_path, "r") as fout:
                self.assertEqual(fout.read(), "test")

            # now run task again
            result = self.op.execute(self.ti.get_template_context())
            self.assertEqual(result, output_path)  # result will still give path
            self.assertTrue(self.op.previously_completed)

            # if function had run again, it would now be 'testtest'
            with open(output_path, "r") as fout:
                self.assertEqual(fout.read(), "test")

            # run function again to ensure 'testtest' is written to file upon second call
            self.f(output_path)
            with open(output_path, "r") as fout:
                self.assertEqual(fout.read(), "testtest")


class PythonIdempatomicFileOperatorTest_Atomic(unittest.TestCase):
    def g(self, output_path):
        with open(output_path, "w") as fout:
            fout.write("test")
            raise ValueError("You cannot write that!")

    def test_PyIdempaOp_atomic(self):
        self.dag = DAG(
            TEST_DAG_ID,
            schedule_interval="@daily",
            default_args={"start_date": datetime.now()},
        )

        with TemporaryDirectory() as tempdir:
            output_path = f"{tempdir}/test.txt"

            self.assertFalse(
                os.path.exists(output_path)
            )  # ensure doesn't already exist

            self.op = PythonIdempatomicFileOperator(
                dag=self.dag,
                task_id="test",
                output_pattern=output_path,
                python_callable=self.g,
            )
            self.ti = TaskInstance(task=self.op, execution_date=datetime.now())

        with self.assertRaises(
            ValueError
        ):  # ensure ValueError is triggered (since task ran)
            result = self.op.execute(self.ti.get_template_context())
            self.assertEqual(result, None)  # make sure no path is returned

        self.assertFalse(os.path.exists(output_path))  # no partially written file
