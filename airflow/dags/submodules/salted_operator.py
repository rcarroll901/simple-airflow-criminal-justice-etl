from .operator import PythonIdempatomicFileOperator
from hashlib import sha256
from datetime import datetime
from functools import wraps

# to version python callable, use @version('1.2.3') decorator
def version(version_num):
    def decorator(func):
        func._version = version_num
        return func

    return decorator


class PythonSaltedLocalOperator(PythonIdempatomicFileOperator):
    """

    Executes a Python callable as a part of an immutable workflow.

    In other words: creates a file or directory such that

    (1) If the file or dir already exists, the python_callable is skipped.

    (2) The creation is atomic, in that, if writing or creation is not completed,
        the partial file or dir will be cleaned up.

    (3) A salt is computed and substituted into the output_pattern. This salt detects
        changes in logic (i.e. callable version) or data (i.e. op_kwargs) upstream  and re-runs task
        when changes are found

    Note: op_args from PythonIdempatomicFileOperator were removed for ease of salting. Please use
        op_kwargs for any/all python_callable inputs

    :param python_callable: A reference to an object that is callable
    :type python_callable: python callable which must take 'output_path' as a parameter
    :param output_pattern: A templated file/directory path where the {vars} in pattern correspond
        to op_kwargs. In order for salt to be applied, '{salt}' must be in output_pattern.
    :param op_kwargs: a dictionary of keyword arguments that will get unpacked
        in your function
    :type op_kwargs: dict (templated)
    :param templates_dict: a dictionary where the values are templates that
        will get templated by the Airflow engine sometime between
        ``__init__`` and ``execute`` takes place and are made available
        in your callable's context after the template has been applied. (templated)
    :type templates_dict: dict[str]
    :param templates_exts: a list of file extensions to resolve while
        processing templated fields, for examples ``['.sql', '.hql']``
    :type templates_exts: list[str]
    """

    def __init__(
        self,
        python_callable,
        op_kwargs={},
        output_pattern=None,
        templates_dict=None,
        templates_exts=None,
        *args,
        **kwargs,
    ):

        super().__init__(
            python_callable=python_callable,
            output_pattern=output_pattern,
            op_kwargs=op_kwargs,  # force user to use kwargs to avoid arg annoyances
            templates_dict=templates_dict,
            templates_exts=templates_exts,
            *args,
            **kwargs,
        )
        self.previously_completed = (
            None  # this will allow us to easily check run status
        )

        # python_callable must have version
        try:
            self.python_callable._version
        except:
            raise AttributeError(
                "Must version python_callables to use salted workflow. See @version decorator."
            )

    def get_salted_version(self, context):
        """
        Get salt for current task given adjacent, upstream tasks using kwargs and python_callable
        info
        """
        ti = context["ti"]  # access metadate
        upstream = list(self.upstream_task_ids)  # property of BaseOperator
        upstream.sort()  # put in alphabetical order just in case variance

        # just grab the salts of directly upstream tasks directly from xcom
        msg = "".join([ti.xcom_pull(task_id, key="salt") for task_id in upstream])

        # uniquely identify this task
        self.python_callable_version = self.python_callable._version
        self.kwargs_rep = ",".join(
            [f"{key}={value}" for key, value in self.op_kwargs.items()]
        )

        # append info to upstream salts
        msg += ",".join([self.task_id, self.python_callable_version, self.kwargs_rep])

        # return hex repr
        return sha256(msg.encode()).hexdigest()

    def get_file_path(self):
        """
        Substitute op_kwargs and salt into output_pattern
        """
        return self.output_pattern.format(
            salt=self.salt[:6],
            **self.op_kwargs,
            today_date=datetime.today().strftime("%m-%d-%Y"),
        )

    def log_run_data(self, context):
        """
        Push the python_callable version, kwargs, and salt to Xcom for use/reference downstream
        """
        ti = context["ti"]

        # version of callable
        self.log.info("PYTHON_CALLABLE VERSION: " + self.python_callable_version)
        ti.xcom_push(key="python_callable_version", value=self.python_callable_version)

        # kwargs
        self.log.info("KWARGS: " + self.kwargs_rep)
        ti.xcom_push(key="kwargs", value=self.kwargs_rep)

        # salted version
        self.log.info("SALT: " + self.salt)
        ti.xcom_push(key="salt", value=self.salt)

    def execute(self, context):
        """
        Executes the logic of the operator
        """

        # get the salt which will be passed into execute() automatically
        self.salt = self.get_salted_version(context)
        super().execute(context)
        self.log.info("SALTED PATH: " + self.output_path)

        # push relevant variables to Xcom for reference/use downstream
        self.log_run_data(context)
