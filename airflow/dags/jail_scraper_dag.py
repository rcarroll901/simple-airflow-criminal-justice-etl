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
from submodules.operator import PythonIdempatomicFileOperator
from submodules.task import requires
from jail_scraper.airflow_scraper import main as scrape_jail_website
from odyssey_scraper.smartsearch import SmartSearchScraper


def scrape_jail(output_path, test):
    """
    This task scrapes the entirety of this website and puts it into multiple files that correspond
    to our normalized database schema. These files will be uploaded to the database after we check
    if they already have a profile
    """
    scrape_jail_website(scrape_dir=output_path, test=test)
    return output_path


def check_jail_profiles(output_path, **kwargs):
    """
    In production, this task will check the list received from the scrape_jail task to see who
    already has a profile in our database. For those who do not, it will produce a list, and it will
    then divide that list among the appropriate amount of workers via csv files.

    To keep our dag simple for the final project, I am simply dividing tasks up among the workers
    and not checking their status in the database
    """

    # load filepaths from required task
    reqs = requires("scrape_jail", **kwargs)
    logging.info("Requirements:", str(reqs))

    # get info for people in jail (which is stored in people.csv in dir created by 'scrape_jail')
    with open(reqs["people"], "r", newline="") as fout:
        data = list(csv.reader(fout))
    logging.info("opened people.csv")

    # user decides scrapes_per_worker depending on personal preference and number of scrapers
    scrapes_per_worker = int(Variable.get("scrapes_per_worker", default_var=3))
    logging.info("scrapes_per_worker = " + str(scrapes_per_worker))

    # how many people need their profiles scraped
    num_people_to_scrape = len(data)
    logging.info("num_people_to_scrape = " + str(num_people_to_scrape))

    # this determines how many tasks we create to do the scraping (which allows it to be done in
    # parallel when deployed)
    num_tasks = math.ceil(num_people_to_scrape / scrapes_per_worker)
    logging.info("num_tasks = " + str(num_tasks))

    # split big list of people into 'to do lists" for each of the workers. If worker fails, it can
    # be re-run and pull in exact same people
    for x in range(num_tasks):
        chunk = data[x * scrapes_per_worker + 1 : (x + 1) * scrapes_per_worker + 1]
        out_path = os.path.join(output_path, f"todo_{x}.csv")
        with open(out_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerows(chunk)
        logging.info(f"wrote todo_{x}")

    # set variable in Airflow (stored in meta-db) to use in constructing dynamic DAG later
    Variable.set("num_odyssey_scraping_tasks", num_tasks)

    # set variable that controls concurrency (basically concurrency proportial to percentage of scrapes up to 1000)
    max_concurrency = os.cpu_count() - 1
    concurrency = (
        math.ceil(max_concurrency * num_tasks / 1000)
        if num_tasks / 1000 < 1
        else max_concurrency
    )
    Variable.set("concurrency", concurrency)

    return "Complete"


def scrape_odyssey(index, output_path, **kwargs):
    """
    This task represents a single scraper which grabs a "to do" list of people to scrape from the
    previous task, logs into Odyssey Criminal Justice Portal, scrapes each persons information,
    and then logs it into csv files.

    :param index: the ID number of the worker (which corresponds to the list it reads)
    """

    logging.info(f"running scraper {index}")

    out_path = output_path
    reqs = requires("check_profiles", **kwargs)

    # read in "to do list"
    with open(reqs[f"todo_{index}"], "r", newline="") as fout:
        data = list(csv.reader(fout))

    # log in information for Odyssey Criminal Justice Portal (Shelby County, TN)
    odyssey_user = os.environ["ODYSSEY_USER"]
    odyssey_pwd = os.environ["ODYSSEY_PASS"]
    # scraper that I developed for this system
    scr = SmartSearchScraper(odyssey_user, odyssey_pwd)

    total_cases = []
    for person in data:
        logging.info(str(person))
        name = person[1]
        dob = person[2]
        justice_history = scr.query_name_dob(name, dob, get_rni=True)
        for case in justice_history.case_grid_list():
            # combine all a person's cases into a list of lists (instead of a list of list of lists)
            # to get one charge per row
            total_cases += case
    scr.quit()

    with open(out_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(total_cases)

    return "Complete"


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

dag = DAG("jail_scraper_dag", default_args=default_args, schedule_interval="@once",)


jail_scraper = PythonIdempatomicFileOperator(
    task_id="scrape_jail",
    python_callable=scrape_jail,
    output_pattern="data/scrapes/{today_date}/jail_scrape/",
    op_kwargs={"test": True},
    dag=dag,
)

check_profiles = PythonIdempatomicFileOperator(
    task_id="check_profiles",
    python_callable=check_jail_profiles,
    output_pattern="data/scrapes/{today_date}/to_do/",
    provide_context=True,
    dag=dag,
)

upload = PythonOperator(task_id="upload_data", python_callable=upload_data, dag=dag)


# pull the variable from Airflow (which was set in previous task)
# wrapping this in a subdag operator will be best moving forward and allows us to adjust the
# concurrency of these scrapers mid-dag_run. Without SubDagOperator, we cannot control concurrency
# after the dag is instantiated
num_tasks = int(Variable.get("num_odyssey_scraping_tasks", default_var=1))
for i in range(num_tasks):
    odyssey_scraper = PythonIdempatomicFileOperator(
        task_id="odyssey_scraper_" + str(i),
        output_pattern="data/scrapes/{today_date}/worker_{index}/cases.csv",
        dag=dag,
        python_callable=scrape_odyssey,
        provide_context=True,
        op_kwargs={"index": i},
    )

    check_profiles.set_downstream(odyssey_scraper)
    odyssey_scraper.set_downstream(upload)


jail_scraper >> check_profiles
