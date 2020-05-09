
from .operator import PythonIdempatomicFileOperator
from hashlib import sha256
from datetime import datetime

# to version python callable, use @version('1.2.3') decorator
def version(version_num):
    def decorator(func):
        func.__version__ = version_num
        return func
    return decorator

class PythonSaltedLocalOperator(PythonIdempatomicFileOperator):

    def __init__(
        self,
        python_callable,
        op_kwargs={},
        output_pattern=None,
        templates_dict=None,
        templates_exts=None,
        *args,
        **kwargs
    ):

        super().__init__(
            python_callable=python_callable,
            output_pattern = output_pattern,
            op_kwargs=op_kwargs, # force user to use kwargs to avoid arg annoyances
            templates_dict=templates_dict,
            templates_exts=templates_exts,
            *args,
            **kwargs
        )
        self.previously_completed = (
            None  # this will allow us to easily check run status
        )

        # python_callable must have version
        try:
            self.python_callable.__version__
        except:
            raise AttributeError(
                'Must version python_callables to use salted workflow. See @version decorator.')


    def get_salted_version(self, context):
        ti = context['ti'] # access metadate
        upstream = list(self.upstream_task_ids) # property of BaseOperator
        upstream.sort() # put in alphabetical order just in case variance

        # just grab the salts of directly upstream tasks directly from xcom
        msg = ''.join([ti.xcom_pull(task_id, key='salt') for task_id in upstream])

        # uniquely identify this task
        self.python_callable_version = self.python_callable.__version__
        self.kwargs_rep = ','.join([f'{key}={value}' for key, value in self.op_kwargs.items()])
        
        # append info to upstream salts
        msg += ','.join([self.task_id, self.python_callable_version, self.kwargs_rep])
        
        # return hex repr
        return sha256(msg.encode()).hexdigest()
        

    def get_file_path(self):
        return self.output_pattern.format(salt=self.salt[:6],
                                          **self.op_kwargs, 
                                          today_date=datetime.today().strftime("%m-%d-%Y"))

    def log_run_data(self, context):
        ti = context['ti']

        # version of callable
        ti.xcom_push(key='python_callable_version', value=self.python_callable_version)

        # kwargs
        ti.xcom_push(key='kwargs', value=self.kwargs_rep)

        # salted version
        ti.xcom_push(key='salt', value=self.salt)

    def execute(self, context):
        self.salt = self.get_salted_version(context)
        super().execute(context)
        self.log.info('SALTED PATH: ' + self.output_path)
        self.log_run_data(context)
