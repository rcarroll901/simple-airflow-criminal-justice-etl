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

#### Setup
Airflow is a little fidgetty to set up, so I mended together Dockerfiles/docker-compose.yaml's to
produce a reproducible environment which runs Airflow and allows scraping using Selenium Webdriver.

To get the docker network running (which consists of `postgres:11.7` and `python:3.7` images), we first
need to defined a `.env` file with:

```
AIRFLOW_HOME=/usr/local/airflow # where Airflow lives in container
CI_USER_TOKEN=... # user token to access my private scraping repos in GitHub
ODYSSEY_USER=... # username for Odyssey Criminal Justice Portal
ODYSSEY_PASS=... # password for Odyssey
```

Then, we simply run `$ docker-compose up` which automatically runs Airflow's webserver and scheduler
upon entry into the container. Opening up a browser, you can connect to localhost:8080 to see the
Airflow UI. To kick off the dag, we simply hit the play button on the right side. 



