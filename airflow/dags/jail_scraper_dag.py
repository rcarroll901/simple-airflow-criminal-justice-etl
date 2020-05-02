import datetime as dt
import os
import math
import csv
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

from jail_scraper.airflow_scraper import main
from datetime import datetime

from odyssey_scraper.smartsearch import SmartSearchScraper

SCRAPE_DIR = "data/scrapes/" + datetime.today().strftime("%m-%d-%Y") + "/"

def check_jail_profiles():

    with open(SCRAPE_DIR + "people.csv", "r", newline="") as fout:
        data = list(csv.reader(fout))

    logging.info("opened people.csv")

    # data = check(data)

    scrapes_per_worker = int(Variable.get("scrapes_per_worker", default_var=3))
    logging.info("scrapes_per_worker = " + str(scrapes_per_worker))

    num_people_to_scrape = len(data)
    logging.info("num_people_to_scrape = " + str(num_people_to_scrape))

    num_tasks = math.ceil(num_people_to_scrape/scrapes_per_worker)
    logging.info("num_tasks = " + str(num_tasks))

    # MAKE THIS ATOMIC
    for x in range(num_tasks):
        chunk = data[x*scrapes_per_worker+1:(x+1)*scrapes_per_worker+1]
        out_path = os.path.join(SCRAPE_DIR, f"todo_{x}.csv")
        with open(out_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerows(chunk)
        logging.info(f"wrote todo_{x}")

    # set variable in Airflow to use in constructing dynamic DAG
    os.system('airflow variables --set num_odyssey_scraping_tasks ' + str(num_tasks))


def my_callable(index):

    out_path = SCRAPE_DIR + f"/worker_{index}/cases.csv"

    # if already exists, then skip
    if os.path.exists(out_path):
        return 'Previously Completed'

    # if it doesn't exist, scrape
    logging.info(f'running scraper {index}')
    with open(SCRAPE_DIR + f"todo_{index}.csv", "r", newline="") as fout:
        data = list(csv.reader(fout))


    odyssey_user = os.environ['ODYSSEY_USER']
    odyssey_pwd = os.environ['ODYSSEY_PASS']
    scr = SmartSearchScraper(odyssey_user, odyssey_pwd)

    total_cases = []
    for person in data:
        logging.info(str(person))
        name = person[1]
        dob = person[2]
        justice_history = scr.query_name_dob(name, dob, get_rni=True)
        for case in justice_history.case_grid_list():
            total_cases.append(case)
    scr.quit()


    logging.info('writing ' + out_path)
    if not os.path.exists(out_path):
        os.makedirs(os.path.dirname(out_path))

    with open(out_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(total_cases)

    return 'Completed'


def upload_data():
    pass

default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2020, 4, 30, 16, 00, 00),
    'concurrency': 1,
    'retries': 0
}

dag =  DAG('jail_scraper_dag',
         default_args=default_args,
         schedule_interval='@once',
         )


scrape_jail = PythonOperator(task_id='scrape_jail',
                            python_callable=main,
                            op_kwargs={'test':True},
                            dag = dag)

check_profiles = PythonOperator(task_id='check_profiles',
                                 python_callable=check_jail_profiles,
                                dag = dag)

num_tasks = int(Variable.get("num_odyssey_scraping_tasks", default_var=4))
for i in range(num_tasks):
    odyssey_scraper = PythonOperator(task_id='odyssey_scraper_'+str(i),
                                    dag = dag,
                                    python_callable=my_callable,
                                    op_args=[i])

    check_profiles.set_downstream(odyssey_scraper)
    # odyssey_scraper.set_downstream(upload_task)

#scrape_odyssey = PythonOperator(task_id='scrape_odyssey',
#                             python_callable=respond)



scrape_jail >> check_profiles



if __name__ == "__main__":
    check_jail_profiles()