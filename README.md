# Criminal Justice ETL (+ Extending Apache Airflow)

___
Table of Contents
=================

   * [airflow_cj_report_card](#airflow_cj_report_card)
      * [Motivation](#motivation)
      * [PythonIdempatomicFileOperator](#pythonidempatomicfileoperator)
      * [requires()](#requires)
      * [PythonSaltedLocalOperator and a Salted DAG](#salted-dags)
      * [Implementation of PythonIdempatomicFileOperator](#implementation-of-pythonidempatomicfileoperator)
        * [Mini-Criminal Justice Scraping Pipeline](#mini-criminal-justice-scraping-pipeline)
        * [Preface](#preface)
        * [Data](#data)
        * [Setup](#setup)
        * [About the DAG](#about-the-dag)
        * [Deployment](#deployment)
        * [Other](#other)
        * [The Future](#the-future)
      * [Testing](#testing)
      
## Motivation

This repository is a simplfied version of a project that I've been working toward for [Just City](https://justcity.org/), a local criminal justice reform organization in Memphis, TN. I've been scraping very granular jail and criminal history data for a while now, and when I started my project, I planned to use Airflow to pull together this big pipeline and do the (quite annoying) order of operations required to get a properly normalized, queryable DB structure. But after working with Airflow for a while, I realized something:

I don't like Airflow. 

At least, I should say that I don't like a few specific (lack of) features. In particular, focusing on the `PythonOperator`, tasks in all the Aiflow docs and exmaples are almost always a plain old python function which causes us to lose a lot of the nice, clean (but also rigid/opinionated) functionality that Luigi gave us. 

For example:
1. **Inter-Task Communication:** It was really nice that we could refer to a task by name and grab the `output().path` to get the data and continue the pipeline without having to remember the exact (and probably dynamic) path of that output. In Airflow, if you want to read an output file of another task out of the box, you have to hardcode that into the task or do some ugly logic to automate it. If that output path changes, you will most likely have to manually change all of the references to that file throughout other tasks. 
2. **Idempotency:** While it did have its downfalls (which were mostly resolved with the "salted graph" paradigm), the fact that Luigi automatically knew not to re-run a task if the output already existed was very useful, especially if you are running a scraper that takes 20 minutes to 5 hours.
3. **Atomicity:** As we learned, this is an incredibly important part of our workflow. If one file is partially made and goes into production, it can corrupt our data in a way that is very difficult to notice or retroactively fix. Airflow does not provide any functionality that ensures atomicity in the outputs of the tasks. In fact, in the documentation, Airflow even says that operators are "usually (but not always) atomic", but they mean "[operators] can stand on their own and don’t need to share resources with any other operators" -- not *actually* atomic in the way that we want.

We could (and, in hindsight, should....) copy the class/method-based tasks format of Luigi, but at the time, I liked the cleanliness of using plain python functions and decided to extend it in this more basic form. Also, I'm **100% certain** that there are more clever and potentially obvious ways using only Apache Airflow features out of the box to accomplish all of these things, but I was excited to dig in and implement my own attempt in the name of problem solving and curiosity.

This repo contains a few extensions of Apache Airflow to add some of my favorite Luigi "opinions":

* Change the DAG dynamically depending on the result of an upstream task. Around the time that this repo was created and written, the `SubDagOperator` did not meet my usecase due to the differences in scheduling the dag/subdag but, with developments since that time, is now the clearly superior route for my usecase
* Implements and showcases a new operator --  `PythonIdempatomicFileOperator` (or, as I like to call it, the `PythonYoshiOperator`) -- and paradigm in Airflow that provides a solution to the above "problems" in a user-friendly way, keeping the flexibility (and scalability, testability, and the UI!) of Airflow while adding some of the conveniences/cleanliness of Luigi. 
* Implements and showcases a function  -- `requires()` -- to communicate upstream file dependency locations to downstream tasks using Airflow's Xcom feature. As mentioned above, Airflow tasks are supposed to be "atomic" in the sense that they stand alone, but what if the upstream task changes the location of the output file? We then have a tight coupling of the tasks and we have to manually change the downstream code to look in that new location.
* Implements and showcases an extension of the above oeprator called the `PythonSaltedLocalOperator` which detects changes in nodes of the DAG via an explicit `@version` decorator on the executables + an imlicit detection of changed paramters in the Task definitions. Since I ended up implementing a "salted dag" workflow, I also added a demonstration video of the `jail_scraper_dag` (without any actual scraping) using this operator.

Below are my powerpoint presentation and videos which give varying levels of insight into this project and results:

PowerPoint: [Presentation.pdf](https://drive.google.com/open?id=1KWzkc_oH4y3ZZtKfdKIHQnIiEJXi9ey-)

Intro Video (Optional): [Intro to Airflow and Context.mp4](https://drive.google.com/open?id=1DybmV_X64INPTUr6kPCcA9N7a1EOfzAe)

Overview Video: [Overview.mp4](https://drive.google.com/open?id=1-oNeSJOUFifrcbP0DT5zhbx50xvYqQLM)

Salted Workflow: [Airflow Salted Dag Demo](https://drive.google.com/file/d/1NCba2v2u3mYdKWAANYTHeYgOoQWJZXER/view?usp=sharing)



## PythonIdempatomicFileOperator

**source code:** `airflow/dags/submodules/operator.py`

**tests:** `airflow/dags/submodules/test_operator.py`

From the user experience, implementing the `PythonIdempatomicFileOperator` is almost identical to 
implementing the plain `PythonOperator`:

**PythonOperator:**
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
1. Add `output_pattern` kwarg to the operator instantiation
2. Add `output_path` arg to the definition of the python callable

**PythonIdempatomicFileOperator:**
```python
def task(output_path):
    create_my_files(output_path) # creates file_1.csv and file_2.csv in /foo/bar/ directory
    return 'Complete'

my_task = PythonIdempatomicFileOperator(task_id='task_1',
                                        python_callable = task,
                                        output_pattern='/foo/bar/', 
                                        dag = dag)
```

With adding an `output_path` argument to our task  and the `output_pattern` argument to our operator 
(which will be passed to the `output_path` argument in `task` automatically by our operator), we gain
both atomicity and idempotency automatically. For more details on how this was implemented, please 
see the PowerPoint linked above which goes into the source code and also draws a side-by-side
comparison of the hardcoded way of implementing these concepts. 

**Important Notes:**
* Directories must always include an ending `/` or `\` (depending on operating system) to designate
that it is not just a file without an extension.
* When a task is skipped, it is not highlighted pink in Airflow's UI. It is still considered a 
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
1. Add `provide_context = True` kwarg to operator instantiation. This gives us access to Airflow metadata about the DAG and "dagrun".
2. Add `**kwargs` argument to python_callable. These kwargs are the metadata and are automatically put into the callable for us.

```python
def task_2(output_path, **kwargs): # kwargs are the metadata passed in from provide_context

    # and we just pass the kwargs through to requires
    req_files = requires('task_1', **kwargs) # returns {'file_1': '/foo/bar/file_1.csv', 'file_2': '/foo/bar/file_2.csv'}
    file_1_path = req_files['file_1'] # this is our file path to 'file_1.csv'
    
    # then with data pipeline
    df = pandas.read_csv(file_1_path)
    df.to_parquet(output_path)
    return 'Complete'

my_second_task = PythonIdempatomicFileOperator(task_id='task_2',
                                            python_callable = task_2,
                                            output_pattern='/foo/',
                                            provide_context = True, # gives Airflow's metadata!
                                            dag = dag)
```


Important Notes:
* When given a task whose output path is a directory, `requires` does not search that directory recursively, 
so it will only return the files and subdirectory names in that directory, but it will not include files in those subdirectories. 
* Alluded to above, `requires` uses Xcom to grab the information that is returned from an operator. 
For PythonIdempatomicOperator, the `output_path` is *always* returned and is pushed to Xcom, regardless of what the return 
statement in the `python_callable` returns. Since these return statements are usually just used for 
status logs, I felt comfortable overwriting the user's ability to return their own result. Although, the 
return value from the python_callable that the user returns is still logged in the PythonIdempatomicFileOperator. 
It is simply just not "returned" and pushed to Xcom.

## PythonSaltedLocalOperator and a Salted DAG

**source code:** `airflow/dags/submodules/salted_operator.py`

**sandbox dag:** `airflow/dags/salted_dag.py`

**demo:** [Airflow Salted Dag Workflow Demonstration](https://drive.google.com/file/d/1NCba2v2u3mYdKWAANYTHeYgOoQWJZXER/view?usp=sharing)

After completing the assignment, I decided to give an hour to building a `PythonSaltedLocalOperator`
(I'm terrible at naming things) as a child class of `PythonIdempatomicFileOperator` with a lot of
 success. Compared to the above examples, it looks like this:

```python
@version('0.1.2')
def task(output_path, my_kwarg):
    create_my_files(output_path) # creates file_1.csv and file_2.csv in /foo/bar/ directory
    return 'Complete'

my_task = PythonSaltedLocalOperator(task_id='task_1',
                                        python_callable = task,
                                        output_pattern='/foo/bar-{salt}/', 
                                        op_kwargs = {'my_kwarg':4}
                                        dag = dag)
```

So, all we had to add was a `@version` decorator (imported from `salted_operator.py`) to our python callable and add a place in the
`output_pattern` template for the salt to be placed. The salt detects changes in the kwargs and the version numbers,
and then passes that information downstream using Xcom. Xcom acts kind of like the cache which Prof Gorlin
mentioned in class, allowing our computation to be O(N) vs O(N^2) since finding the salt does not 
need to be defined recursively (even though caching in DB probably increases the amount of time 
overall). We can just simply grab the salt for each task from the meta-database.

I also added an `example_salted_dag` DAG into `airflow/dags/salted-dag.py` which is basically a 
hollowed out version of the criminal justice scraping dag (i.e. all the logic from the actual tasks
is removed. I mention in the criminal justice scraping section below that the user must download some
private repos to test run the scraper. Since the task logic is hollowed out, this `example_salted_dag`
can **almost** be run out of the box, and the user can see the PythonIdempatomicFileOperator behavior
(skipping completed tasks and writing atomically) *and* the salted behavior without going through all 
the trouble of getting the dag below set up. 

In order to run the `example_salted_dag`, simply remove the custom private repos from the Pipfile, and 
then use `$ docker-compose build` and subsequently `$ docker-compose up` to get the scheduler and webserver 
up and running. Then go to `localhost:8080` and click the `example_salted_dag` from "OFF" to "ON" and hit the play button
in the menu on the right side to officially trigger it. It will probably throw an error saying that 
`jail_scraper_dag` could not be imported (since dependencies aren't there), but you can just delete 
`jail_scraper_dag.py` out of your forked repo if it prevents you from running the example dag independently.


## Implementation of PythonIdempatomicFileOperator

#### Mini-Criminal Justice Scraping Pipeline

**source code:** `airflow/dags/jail_scraper_dag.py`

**tests:** `airflow/dags/test_dag.py`

#### Preface

**Goal:** Get criminal history information on people who are incarcerated pretrial in Shelby County, TN. 
First, we want to scrape the jail population, and then use their name and DOB to search them in the
larger database which includes all past court cases (that haven't been expunged).

**Why:** I am going to take this data and implement a django web app which visualizes various stats about 
recidivism, length of stay pretrial, etc... 

**Note:** The portals that are scraped in this pipeline are both completely public information. Just one of 
them requires registration (which doesn't even have a terms of service...), so I have set up dummy 
credentials if anyone wants to poke around. Also, I keep these repos private because I don't want someone using this info
against any of the people incarcerated. I am happy to invite you as a collaborator on the repos so 
so that you can run this pipeline as long as you promise not to misuse it (and as long as you promise
not to judge how ugly the code is. They are relics from a long time ago.)

#### Data
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
is defined at the instantiation of the DAG, meaning that it cannot be changed while the DAG is running.
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
* In the `jail_scraper_dag`, the `output_path` is generated dynamically with the current date at runtime. This date should be
taken as a parameter-like variable so that the data is appropriately labeled if data loading/scraping takes until the next day (Great question on the midterm!).
* `requires()` needs to take into account the salt of the path when it references a task that produces a file, in the sense that, if I want to get the file 
`foo/bar/cases-lb42d4.csv` produced by 'cases_task' then I want to make sure it can be referenced by `cases_file = requires('cases_task', **kwargs)['cases']` 
and not `cases_file = requires('cases_task', **kwargs)['cases-lb42d4']` (which is what it
would currently require). This is only a problem with the case of a file though. The directory case works fine since the salt is applied to the dir and not the files within.
* Persist Airflow meta-db to AWS RDBS: This would allow increased mobility when deploying. Easily taking down and putting up DB without losing history.
* I would like to implement functionality in the PythonIdempatomicFileOperator that shows skipped tasks as "Skipped" in the meta-db. This is easy to do for
upstream and downstream tasks, but it is nontrivial to set the status while the operator is running. I found a way using the AirflowSkipException, but it didn't push the 
 logs to the webserver (since the Operator execution is interrupted).
* Deploy on AWS and scale to cluster using Airflow's CeleryExecutor
* Django webapp for results hosted on Just City's website
* API to give national stakeholders access to good, clean data from one of the epicenters of criminal justice misuse
* Better ability to detect file vs dir from `output_pattern`. I would like for the user to be able to leave off
the trailing '/' and it still detect that it is a directory (as best it can). It's not immediately 
clear how to do this since some files don't have extensions, but I think this is rare enough to 
leverage looking for an extension for better detection (or just simply a warning).
* Add a warning such that, if PythonIdempatomicFileOperator detects an not-explicitly-idempotent/atomic
operator upstream, it throws a warning to the user. If a non-idempotent operator is upstream from 
our idempotent operator, then a DAG re-run would rerun the upstream task but not re-reun the idempotent 
operator. This could be solved with a "signature" as Prof Gorlin described in class (and could be applied
to the "salted case" also), but in the near future, since I'm the only user, a simple (but loud) 
warning is sufficient. 
* I'll probably change the fact that PythonIdempatomicFileOperator always returns the file path. 
I thought it was clever at the time, but it would be better to let people return whatever they want
and push the output_path to Xcom explicitly/separately.

## Testing
In order to execute tests on the DAG, PythonIdempatomicOperator, and requires(), simply run
`$ docker-compose run webserver pytest dags -v` (assuming you have already run `$ docker-compose build`).
`
