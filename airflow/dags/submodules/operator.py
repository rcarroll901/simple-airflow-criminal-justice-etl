from airflow.operators.python_operator import PythonOperator
from airflow.models import SkipMixin
import inspect
import os
from csci_utils.io import atomic_write, atomic_dir_create
from datetime import datetime
from airflow.exceptions import AirflowSkipException


class PythonIdempatomicFileOperator(PythonOperator, SkipMixin):
    """

    Executes a Python callable which creates a file or directory such that

    (1) If the file or dir already exists, the python_callable is skipped.

    (2) The creation is atomic, in that, if writing or creation is not completed, the partial file or dir will be cleaned up.


    :param python_callable: A reference to an object that is callable
    :type python_callable: python callable which must take 'output_path' as a parameter
    :param op_kwargs: a dictionary of keyword arguments that will get unpacked
        in your function
    :type op_kwargs: dict (templated)
    :param op_args: a list of positional arguments that will get unpacked when
        calling your callable
    :type op_args: list (templated)
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
        op_args={},
        op_kwargs={},
        output_pattern=None,
        templates_dict=None,
        templates_exts=None,
        *args,
        **kwargs
    ):
        super().__init__(
            python_callable=python_callable,
            op_args=op_args,
            op_kwargs=op_kwargs,
            templates_dict=templates_dict,
            templates_exts=templates_exts,
            *args,
            **kwargs
        )
        if "output_path" not in inspect.signature(python_callable).parameters:
            raise ValueError("python_callable must have kwarg output_path")
        if output_pattern is None:
            raise ValueError("output_pattern is required")
        if "output_path" in op_kwargs:
            raise ValueError(
                "output_path argument should go in Operator's output_pattern argument (and not in op_kwargs)"
            )
        self.output_pattern = output_pattern
        self.previously_completed = (
            None  # this will allow us to easily check run status
        )

    def get_file_path(self):
        """
        Use kwargs to fill in output_pattern.

        "{today_date}" is automatically understood in output pattern to mean today's date
        """
        return self.output_pattern.format(
            **self.op_kwargs, today_date=datetime.today().strftime("%m-%d-%Y")
        )


    def execute_callable(self):
        """
        Calls the python callable with the given arguments. Replaces the real path with a temp path
        and then moves that temp file to self.output_path when write is completed

        :return: the output path
        :rtype: any
        """

        # if path is a file
        if not self.output_path.endswith(os.sep):
            # create all directories above output file
            parent_dir = os.path.dirname(self.output_path)
            if not os.path.exists(parent_dir):
                os.makedirs(parent_dir)
            # write atomically and insert output_path into python_callable
            with atomic_write(self.output_path, as_file=False) as f:
                self.log.info('DOES TEMP FILE EXIST: ' + str(os.path.exists(f)))
                return self.python_callable(
                    *self.op_args, output_path=f, **self.op_kwargs
                )

        # if path is a directory
        else:
            # create all directories above our output dir (need to remove ending / to
            # get expected behavior)
            parent_dir = os.path.dirname(self.output_path.rstrip(os.sep))
            if not os.path.exists(parent_dir):
                os.makedirs(parent_dir)
            with atomic_dir_create(self.output_path) as d:
                self.log.info('DOES TEMP DIR EXIST: ' + str(os.path.exists(d)))
                return self.python_callable(
                    *self.op_args, output_path=d, **self.op_kwargs
                )


    def execute(self, context):
        self.output_path = self.get_file_path()
        self.log.info('DOES REAL FILE EXIST: ' + str(os.path.exists(self.output_path)))
        self.log.info('DOES REAL FILE EXIST: ' + self.output_path)
        # if file exists already, then task has been completed and skip execution and log
        if os.path.exists(self.output_path):
            self.log.info(
                "This task did not run because it had already been completed."
            )
            self.previously_completed = True  # use this for testing
            log_value = "Previously Completed"

        # if file does not exist, execute per usual
        else:
            self.previously_completed = False
            log_value = super().execute(context)
        self.log.info("Done. Returned value was: %s", log_value)
        self.log.info('DOES REAL EXIST: ' + str(os.path.exists(self.output_path)))
        return (
            self.output_path
        )  # return output_path *always* so it will be logged in Xcom automatic
