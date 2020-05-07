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
can lead to inconsistencies downstream. 

Instead, in my data science workflows, I want atomicity and idempotency to be built in. Hence, the 
`PythonIdempatomicFileOperator`:

PythonIdempatomicFileOperator:
```python
def task(output_path):
    return 'Completed'

my_task = PythonIdempatomicFileOperator(task_id='task_1',
                                        python_callable = task,
                                        output_pattern='/foo/bar/',
                                        dag = dag)
```

With adding an `output_path` argument to our task  and the `output_pattern` argument to our Operatore 
(which will be passed to the `output_path` argument in `task` automatically by our Operator), we gain
both atomicity and idempotency automatically. For more details on how this was implemented, please 
see the PowerPoint linked above.


## requires()
## Mini-Criminal Justice Pipeline