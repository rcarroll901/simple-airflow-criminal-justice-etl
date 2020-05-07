import unittest
from airflow.models import DagBag
from airflow.models import Variable


class TestDagIntegrity(unittest.TestCase):

    LOAD_SECOND_THRESHOLD = 2

    def setUp(self):
        self.dagbag = DagBag()

    def test_import_dags(self):
        self.assertFalse(
            len(self.dagbag.import_errors),
            "DAG import failures. Errors: {}".format(self.dagbag.import_errors),
        )


class TestDagStructure(unittest.TestCase):
    def setUp(self):
        self.dagbag = DagBag()
        dag_id = "jail_scraper_dag"
        self.dag = self.dagbag.get_dag(dag_id)
        self.current_scale_var = int(
            Variable.get("num_odyssey_scraping_tasks", default_var=1)
        )

    def test_num_tasks(self):

        expected = (
            3 + self.current_scale_var
        )  # 3 single tasks and dynamic scaled task depending on var
        self.assertEqual(len(self.dag.tasks), expected)

    def test_dependencies_of_check_profiles_task(self):
        """Check the task dependencies of 'check_profiles' in scraper dag"""
        check_profiles_task = self.dag.get_task("check_profiles")

        downstream_task_ids = list(
            map(lambda task: task.task_id, check_profiles_task.downstream_list)
        )
        downstream_task_ids.sort()

        self.assertListEqual(
            downstream_task_ids,
            [f"odyssey_scraper_{i}" for i in range(self.current_scale_var)],
        )

        upstream_task_ids = list(
            map(lambda task: task.task_id, check_profiles_task.upstream_list)
        )
        self.assertListEqual(upstream_task_ids, ["scrape_jail"])
