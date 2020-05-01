import datetime as dt
import os
import math
import csv
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

from jail_scraper.main import main
from datetime import datetime

from odyssey_scraper.smartsearch import SmartSearchScraper

SCRAPE_DIR = "data/scrapes/" + datetime.today().strftime("%m-%d-%Y") + "/"

def primer_task():
    #os.system('airflow variables --set scrapes_per_worker ' + str(50))
    #os.system('airflow variables --set num_odyssey_scraping_tasks ' + str(1))

def check_jail_profiles():

    with open(SCRAPE_DIR + "people.csv", "r", newline="") as fout:
        data = list(csv.reader(fout))

    logging.info("opened people.csv")

    # data = check(data)
    scrapes_per_worker = int(Variable.get("scrapes_per_worker"))
    logging.info("scrapes_per_worker = " + str(scrapes_per_worker))

    num_people_to_scrape = len(data)
    logging.info("num_people_to_scrape = " + str(num_people_to_scrape))

    num_tasks = math.ceil(num_people_to_scrape/scrapes_per_worker)
    logging.info("num_tasks = " + str(num_tasks))

    # MAKE THIS ATOMIC
    for x in range(num_tasks):
        chunk = data[x*scrapes_per_worker:(x+1)*scrapes_per_worker]
        with open(SCRAPE_DIR + "todo_" + str(x) + ".csv", "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerows(chunk)
        logging.info("wrote todo_" + str(x))

    # set variable in Airflow to use in constructing dynamic DAG
    #os.system('airflow variables --set num_odyssey_scraping_tasks ' + str(num_tasks))


def my_callable(index):
    logging.info('ran_scraper'+str(index))

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

primer = PythonOperator(task_id='primer_task',
                        python_callable=primer_task,
                        dag=dag)

scrape_jail = PythonOperator(task_id='scrape_jail',
                           python_callable=main,
                             dag = dag)

check_profiles = PythonOperator(task_id='check_profiles',
                                 python_callable=check_jail_profiles,
                                dag = dag)

num_tasks = int(Variable.get("num_odyssey_scraping_tasks"))
for i in range(num_tasks):
    odyssey_scraper = PythonOperator(task_id='odyssey_scraper_'+str(i),
                                    dag = dag,
                                    python_callable=my_callable,
                                    op_args=[i])

    check_profiles.set_downstream(odyssey_scraper)
    # odyssey_scraper.set_downstream(upload_task)

#scrape_odyssey = PythonOperator(task_id='scrape_odyssey',
#                             python_callable=respond)





primer >> scrape_jail >> check_profiles



if __name__ == "__main__":
    check_jail_profiles()