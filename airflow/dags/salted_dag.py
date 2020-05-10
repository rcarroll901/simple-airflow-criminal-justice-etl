# standard library
import datetime as dt
from datetime import datetime
import os
import math
import csv
import logging

# external dependencies
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

# custom packages
from submodules.salted_operator import PythonSaltedLocalOperator, version
from submodules.task import requires
from jail_scraper.airflow_scraper import main as scrape_jail_website
from odyssey_scraper.smartsearch import SmartSearchScraper


@version("0.1.0")
def scrape_jail(output_path, test):
    return "Complete"


@version("0.2.0")
def check_jail_profiles(output_path, fake_arg, **kwargs):
    return "Complete"


@version("0.1.0")
def scrape_odyssey(index, output_path, **kwargs):
    return "Complete"


@version("0.1.0")
def upload_data():
    """
    Dummy task to fake upload data
    """
    pass


default_args = {
    "owner": "airflow",
    "start_date": dt.datetime(2020, 4, 30, 16, 00, 00),
    "concurrency": int(Variable.get("concurrency", default_var=1)),
    "retries": 0,
}

dag = DAG("example_salted_dag", default_args=default_args, schedule_interval="@once",)


jail_scraper = PythonSaltedLocalOperator(
    task_id="scrape_jail",
    python_callable=scrape_jail,
    output_pattern="data/scrapes/{today_date}/jail_scrape-{salt}/",
    op_kwargs={"test": True},
    dag=dag,
)

check_profiles = PythonSaltedLocalOperator(
    task_id="check_profiles",
    python_callable=check_jail_profiles,
    output_pattern="data/scrapes/{today_date}/to_do-{salt}/",
    op_kwargs={"fake_arg": 35},
    provide_context=True,
    dag=dag,
)

upload = PythonOperator(task_id="upload_data", python_callable=upload_data, dag=dag)


# pull the variable from Airflow (which was set in previous task)
# wrapping this in a subdag operator will be best moving forward and allows us to adjust the
# concurrency of these scrapers mid-dag_run. Without SubDagOperator, we cannot control concurrency
# after the dag is instantiated
num_tasks = int(Variable.get("num_odyssey_scraping_tasks", default_var=3))
for i in range(num_tasks):
    odyssey_scraper = PythonSaltedLocalOperator(
        task_id="odyssey_scraper_" + str(i),
        output_pattern="data/scrapes/{today_date}/worker_{index}/cases-{salt}.csv",
        dag=dag,
        python_callable=scrape_odyssey,
        provide_context=True,
        op_kwargs={"index": i},
    )

    check_profiles.set_downstream(odyssey_scraper)
    odyssey_scraper.set_downstream(upload)


jail_scraper >> check_profiles
