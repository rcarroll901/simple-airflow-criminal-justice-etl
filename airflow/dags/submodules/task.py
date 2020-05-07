import os

def requires(task_id, **kwargs):
    """
    Uses Airflow's Xcom to pull in return value from another task

    :param task_id: task_id of the task for which we want the return value
    :param kwargs: context **kwargs from Airflow. Automatically input when using kwarg
        provide_context = True in operator kwargs
    :return: dictionary with keys that are the names (without extension) of the files and with
        values that are the full path to that file

    Example:
        if 'task_1' has output path 'data/foo/bar/' produces files called 'file_1.csv', 'file_2.csv'
        Then
            requires('task_1', **kwargs)
        Returns
            {'file_1': 'data/foo/bar/file_1.csv', 'file_2': 'data/foo/bar/file_1.csv'}

    """
    req_dict = {}

    # Use Xcom to pull in return value from provided task_id.
    # Despite it being ugly, this is actually the standard/official way of doing it :(
    output_path = kwargs['ti'].xcom_pull(task_ids=task_id)

    # if output was directory, create a key-value pair for each file in directory
    if output_path.endswith(os.sep):
        files = os.listdir(output_path)
        # run through files in directory
        for file in files:
            name = os.path.splitext(file)[0]
            req_dict[name] = os.path.join(output_path, file)

    # if a file, create a dict with a single entry
    else:
        name = os.path.splitext(os.path.basename(output_path))[0]
        req_dict[name] = output_path

    return req_dict




