# airflow_cj_report_card

## Motivation

My final project relates to a project that I've been working toward for [Just City](https://justcity.org/), a local criminal justice reform organization in Memphis, TN. I've been scraping very granular jail and criminal history data for a while now, and when I started my project, I planned to use Airflow to pull together this big pipeline and do the (quite annoying) order of operations required to get a properly normalized, queryable DB structure. But after working with Airflow for a while, I realized something:

I don't like Airflow. 

At least, I should say that I don't like a few specific (lack of) features. In particular, focusing on the `PythonOperator`, each task is almost always a plain old python function which causes us to lose a lot of the nice, clean functionality that Luigi's class-based tasks gave us. 

For example:
1. **Inter-Task Communication:** It was really nice that we could refer to a task by name and grab the `output().path` to get the data and continue the pipeline without having to remember the exact (and probably dynamic) path of that output. In Airflow, if you want to read an output file of another task out of the box, you have to hardcode that into the task or do some ugly logic to automate it. If that output path changes, you will most likely have to manually change all of the references to that file throughout other tasks. 
2. **Idempotency:** While it did have its downfalls (which were mostly resolved with the "salted graph" paradigm), the fact that Luigi automatically knew not to re-run a task if the output already existed was very useful, especially if you are running a scraper that takes 20 minutes to 5 hours.
3. **Atomicity:** As we learned, this is an incredibly important part of our workflow. If one file is partially made and goes into production, it can corrupt our data in a way that is very difficult to notice or retroactively fix. Airflow does not provide any functionality that ensures atomicity in the outputs of the tasks. In fact, in the documentation, Airflow even says that operators are "usually (but not always) atomic", but they mean "[operators] can stand on their own and don’t need to share resources with any other operators" -- not *actually* atomic in the way that we want.

So, I did build a web scraping pipeline with Airflow that scales the number of scraping tasks (i.e. changes the actual DAG structure) dynamically depending on the result of an upstream task, but, while useful, that is no longer my focus. 

Instead, my final project implements and showcases a new operator --  `PythonIdempatomicFileOperator` (or, as I like to call it, the `PythonYoshiOperator`) -- and paradigm in Airflow that provides a solution to the above "problems" in a user-friendly way, keeping the flexibility (and scalability, testability, and the UI!) of Airflow while adding some of the conveniences/cleanliness of Luigi. 

Below are my powerpoint presentation and videos which give varying levels of insight into my project and results. I had a lot to discuss so I separated the Intro to Airflow/Motivation parts for my project into an "Intro Video" in case you need more context, but the preface video should not be required to understand what I'm doing in the "real" project video. 

PowerPoint: [Final Project - Presentation.pdf](https://drive.google.com/open?id=1KWzkc_oH4y3ZZtKfdKIHQnIiEJXi9ey-)

Intro Video (Optional): [Final Project - Intro to Airflow and Context.mp4](https://drive.google.com/open?id=1DybmV_X64INPTUr6kPCcA9N7a1EOfzAe)

Overview Video: [Final Project - Overview.mp4](https://drive.google.com/open?id=1-oNeSJOUFifrcbP0DT5zhbx50xvYqQLM)

## PythonIdempatomicFileOperator

**source code:** `airflow/dags/submodules/operator.py`
**tests:** `airflow/dags/submodules/test_operator.py`

From the user experience, implementing the `PythonIdempatomicFileOperator` is almost identical to 
implementing the plain `PythonOperator`:

PythonOperator:
```python
def task():
    return 'Completed'

my_task = PythonOperator(task_id='task_1',
                        python_callable = task,
                        dag = dag)
```
where `dag` is a `DAG` object imported from Airflow. This PythonOperator will re-run `task` each time
the operator is executed. In the case of writing to a file, this means it will overwrite or, even worse,
append to the end of the file, causing duplicate entries and possibly corrupting our data, unless the user
was vigilant in preventing this behavior. Additionally, if the writing process in `task` is 
interrupted and the user does not implement an atomic write explicitly, partial files and directories 
can lead to inconsistencies downstream. Therefore, in my data science workflows, I wanted atomicity a
nd idempotency to be built in. 

To implement this operator, we simply add two things to the above paradigm:
1. Add `output_pattern` kwarg to the Operator instantiation
2. Add `output_path` arg to the python_callable

PythonIdempatomicFileOperator:
```python
def task(output_path):
    create_my_files(output_path) # creates file_1.csv and file_2.csv in /foo/bar/ directory
    return 'Complete'

my_task = PythonIdempatomicFileOperator(task_id='task_1',
                                        python_callable = task,
                                        output_pattern='/foo/bar/', 
                                        dag = dag)
```

With adding an `output_path` argument to our task  and the `output_pattern` argument to our Operatore 
(which will be passed to the `output_path` argument in `task` automatically by our Operator), we gain
both atomicity and idempotency automatically. For more details on how this was implemented, please 
see the PowerPoint linked above which goes into the source code and also draws a side-by-side
comparison of the hardcoded way of implementing these concepts 

Important Notes:
* Directories must always include an ending `/` or `\` (depending on operatin system) to designate
that it is not just a file without an extension.
* When a task is skipped, it is not highlighted as Pink in Airflow's UI. It is still considered a 
successful run and is therefore returned as a success.
* Both creating/writing to directories and files are done atomically, regardless of means of creation.
In other words, in task above, we can use `pandas.to_csv` to write the files and it will still ensure
that no partial file is created. 
* `output_pattern` is evaluated using the kwargs of the callable or `date_today` keyword and then fed into 
`task` as output_path. Even if the user does not write to that path, the operator still creates that file
(which acts as a _SUCCESS placeholder of sorts). So even if a file is not written to, the operator will still
behave idempotently and atomically. 


## requires()

**source code:** `airflow/dags/submodules/task.py`
**tests:** `airflow/dags/submodules/test_task.py`

If we are implementing a data pipeline, we will most likely be writing to and reading from a series of
files, databases, etc... as we move and shape that data. Airflow's flexibility does not have an 
immediately obvious way of keeping track of the file paths produced by upstream tasks, and, more 
importantly, keeping those paths automatically updated if the output of an upstream task is moved. 
Therefore, using Xcom, `requires` takes the name of a PythonIdempatomicFileOperator (or any Operator/task
that returns a file path or directory) and returns a dictionary of the paths keyed on file/files on that path
without their extension. 

For example, let's say that we have a new task `my_second_task` which depends on the `my_task` operator
from above and needs the files it creates. Then we can implement `requires` with two simple additions 
to the above example:

To implement `requires()`:
1. Add `provide_context = True` kwarg to operator instantiation. This gives us access to Airflow metadata about dag.
2. Add `**kwargs` argument to python_callable. These kwargs are the metadata and are automatically put into the callable for us.

```python
def task_2(output_path, **kwargs): # kwargs are the metadata passed in from provide_context

    # and we just pass the kwargs through to requires
    req_files = requires('task_1', **kwargs) # returns {'file_1': '/foo/bar/file_1.csv', 'file_2': '/foo/bar/file_2.csv'}
    file_1_path = req_files['file_1']
    df = pandas.read_csv(file_1_path)
    df.to_parquet(output_path)
    return 'Complete'

my_second_task = PythonIdempatomicFileOperator(task_id='task_2',
                                            python_callable = task_2,
                                            output_pattern='/foo/',
                                            provide_context = True, # gives Airflow's metadata!
                                            dag = dag)
```


Important notes:
* requires does not bring search through directories recursively, so it will only return the files
in that directory. Although, it will also have a key for any directories inside that directory.
* alluded to above, requires uses Xcom to grab the information that is returned from an Operator. 
For PythonIdempatomicOperator, the output_path is *always* returned, regardless of what the return 
statement in the python_callable returns. Since these return statements are usually just used for 
status logs, I felt comfortable overwriting the user's ability to return their own messages, and the 
return value from the python_callable is still logged in the PythonIdempatomicFileOperator. It is 
simply just not "returned".



## Implementation: Mini-Criminal Justice Scraping Pipeline

**source code:** `airflow/dags/jail_scraper_dag.py`
**tests:** `airflow/dags/test_dag.py`

#### Preface

**Goal:** Get criminal history information on people who are incarcerated pretrial in Shelby County, TN. 
First, we want to scrape the jail population, and then use their name and dob to search them in the
larger database which includes all past court cases (that haven't been expunged).

**Why:** I am going to take this data and implement a django web app which visualizes various stats about 
recidivism, length of stay pretrial, etc... 

**Note:** The portals that are scraped in this pipeline are both completely public information. Just one of 
them requires registration (which doesn't even have a terms of service...), so I have set up dummy 
credentials if anyone wants to poke around. Also, I keep these repos private because I don't want someone using this info
against any of the people incarcerated. I am happy to invite you as a collaborator on the repos so 
so that you can run this pipeline as long as you promise not to misuse it (and as long as you promise
not to judge how ugly the code is. They are relics from a long time ago.)

#### Data:
* [Jail Population Data](https://imljail.shelbycountytn.gov/IML): Just hit search without putting
anything in the query.
* [Criminal History Data](https://odysseyidentityprovider.tylerhost.net/idp/account/signin?ReturnUrl=%2fidp%2fissue%2fwsfed%3fwa%3dwsignin1.0%26wtrealm%3dhttps%253a%252f%252fcjs.shelbycountytn.gov%252fCJS%252f%26wctx%3drm%253d0%2526id%253dpassive%2526ru%253d%25252fCJS%25252fAccount%25252fLogin%26wct%3d2019-04-10T16%253a27%253a35Z%26wauth%3durn%253a74&wa=wsignin1.0&wtrealm=https%3a%2f%2fcjs.shelbycountytn.gov%2fCJS%2f&wctx=rm%3d0%26id%3dpassive%26ru%3d%252fCJS%252fAccount%252fLogin&wct=2019-04-10T16%3a27%3a35Z&wauth=urn%3a74):
Log in. Click on "Smart Search" and just choose a common name. You can click through their past cases. 

#### Setup
Airflow is a little fidgety to set up, so I mended together Dockerfiles/docker-compose.yaml's to
produce a reproducible environment which runs Airflow and allows scraping using Selenium Webdriver.

To get the docker network running (which consists of `postgres:11.7` and `python:3.7` images), we first
need to define a `.env` file with:

```
AIRFLOW_HOME=/usr/local/airflow # where Airflow lives in container
CI_USER_TOKEN=... # user token to access your GitHub repos once I make you collaborator
ODYSSEY_USER=... # username for Odyssey Criminal Justice Portal
ODYSSEY_PASS=... # password for Odyssey
```


Then, the user simply needs to run `$ docker-compose build` and subsequently `$ docker-compose up` 
which automatically runs Airflow's webserver and scheduler upon entry into the container. Opening 
up a browser, you can connect to localhost:8080 to see the Airflow UI. To kick off the dag, we 
simply hit the play button on the right side. 

Note: Huge thanks to "puckel/docker-airflow" for providing such an outstanding docker setup for getting
Airflow up and running. Much of the base code in the Dockerfile and docker-compose.yaml is their's, 
but I did make significant adjustments to make everything work. Regardless, I would have never been 
able to implement the `entrypoint.sh` bash script without puckel's work though. The parts for Selenium
webdriver were also derived from the Selenium image's Dockerfile, but I actually wrote a lot of it
before finding their code. They just did it better and this isn't a Docker course :) I did have to do
significant work to get pipenv to work with docker-airflow, since it natively worked with a 
requirements.txt file. There were some subtleties with dealing with installing `psycopg2` package
which are well documented online but were difficult to implement in a Dockerfile that the author
clearly had a much deeper knowledge than me. 


#### About the DAG

As summarized before, this dynamic DAG was meant to solve the problem of a scraper failing (which they inevitably
do). By breaking the task of scraping information on 3000 people (max) which can take over 10 hours easily, 
we significantly offset the cost of failure, since when we re-run the dag after that failure, it will skip 
all the tasks that it has already completed and pick up where it left off. 

I also want the ability to scale the number of concurrent workers dynamically in order to reduce
the load on the website when it is unnecessary and to increase the load when I'm worried the 
scrapers will not have enough time to finish the job. I decided to change directions to focus on 
the new operator before I implemented this feature. The current DAG is dynamic in structure, but the concurrency
is defined at the instantiation of the DAG, meaning that it cannot be changed after the DAG runs.
To mitigate this, I will use a SubDagOperator to scale the `odyssey_scraper` tasks which will 
allow me to set the concurrency when those dynamic scraping tasks are kicked off, and I am confident
that it will work straightforwardly. 

In terms of dynamism, Airflow is very interesting, because the scheduler is "filling the DagBag" so
frequently (every 20 seconds or so). This results in a very dynamic -- maybe too dynamic -- workflow, 
because if the code is changed for a downstream task *while the DAG is already running*, it will actually 
run the updated code *in that same dag run* when it gets to that downstream task. Therefore, I need 
to be intentional about when to push code. 

#### Deployment

I am going to proceed with Airflow because of its convenience when it comes to running on remote
servers -- in my case, AWS EC2 instances. While Luigi's UI is sufficient, I find it very convenient 
that I can kick off DAGs, see  logs of individual tasks, etc... without every ssh-ing into the AWS 
instance. Also, notice within my dag that I am using `Variable` objects from Airflow. I am able to change
these variables from the UI, meaning that I can also change the logic of my pipeline without ssh-ing too.
I plan to upgrade this pipeline from a LocalExecutor to a distributed CeleryExecutor for financial reasons (8 t2.micros 
is a quarter of the price of a t3.2xlarge). 

#### Other

During the process of refactoring my jail_scraper code, I also noticed that I had consistently used 
a terrible `try-except` paradigm for scraping to implement my own "try-retry" block. Since javascript 
apps can be inconsistent in their behavior, I basically had:

```python

try:
    scraper.scrape_info(css_selector)
except:
    scraper.scrape_info(css_selector)

``` 
which was quite painful to look back and see. Therefore, I insituted a simple decorator to do this for me:

```python
def try_again_if_timeout(func):

    def wrapper(*args, **kwargs):
        try:
            data = func(*args, **kwargs)
        except TimeoutError:
            data = func(*args, **kwargs)
        return data
    return wrapper
```

This made my code significantly more clean without refactoring almost any code. 

Also, less significantly, I also wrote a version of our original `atomic_write` form Pset1 except for
directories. The idea of using a directory did not seem to mesh with inheriting from `atomicwrites`
package, so I just used the more straightforward approach. It is defined in my `csci-utils`.

#### The Future
* Salted Graph: The PythonIdempatomicFileOperator is prime for implementing a salted graph in Airflow.
Really, all one has to do is inherit and overwrite the `self.get_file_path` method. Additionally, one could 
use an `@version(2.1.1)` decorator such as 
```python
def version(version_num):
    def decorator(func):
        func._version = version_num
        return func
    return decorator
``` 
which would give a version to the python_callable (since we would want the version parameter to be 
closest to where the code is changing). 
* Persist Airflow meta-db to AWS RDBS: This would allow increased mobility when deploying. Easily taking down and putting up DB without losing history.
* Deploy on AWS and scale to cluster
* Django webapp for results hosted on Just City's website
* API to give national stakeholders access to good, clean data from one of the epicenters of criminal justice misuse

