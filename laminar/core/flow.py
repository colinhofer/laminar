from typing import List, Callable, Coroutine, Union, Generic, TypeVar, Type, Unpack
import asyncio

from .task import SyncTask, AsyncTask, TaskType, TaskRun, TaskKwargs
from .run import ConfigType, RunType, Config, FlowRun
from .models import Status
from .exceptions import ErrorThresholdExceeded
from ..http import HTTPRun
from ..database import TransactionLog
from ..logger.logger import Logger

TaskIdentifier = Union[str, TaskType, List[Union[str, TaskType]]]

class Flow(Generic[ConfigType, RunType]):
    """
    Flow class for managing and executing a series of tasks with flexible behavior.
    The Flow class allows for the creation and execution of tasks in a specified order, 
    with support for interval-based tasks, main tasks, and tasks that run at the beginning 
    or end of the flow execution. It also supports locking to prevent concurrent executions.
    Attributes:
        name (str): The name of the flow.
        tasks (List[TaskType]): A list of tasks to be executed in the flow.
        default_config (Config): The default configuration for the flow to be used if no config is passed to `run` or `__call__`.
    Methods:
        __call__(*args, **kwargs): Executes the flow by calling `asyncio.run(self.run())` method. Not recommended in production, as this method is not async and will block other processes, but useful for standalone flows.
        task(name: str = None, run_first: bool = False, run_last: bool = False, main: bool = False, interval_minutes: int = None): 
            Decorator to create a flow task with flexible behavior.
        _run_main(config: Config, run: Run): Executes the main tasks in the flow.
        run(config: Config = None): Executes the flow with the given configuration.
    Example:
        ```python

        DB = "postgres://user:password@localhost:5432/mydatabase"

        my_flow = Flow("MyFlow")

        @my_flow.task(run_first=True, interval_minutes=15)
        async def authenticate(run: Run):
            data = {
                "grant_type": "password",
                "username": run.config.username,
                "password": run.config.api_key
            }
            url = os.path.join(run.config.base_url, "Authenticate")
            response = await run.http.post(url, data=data)
            token = response['access_token'] 
            run.headers = {"Authorization": f"Bearer {token}"}

        @my_flow.task(main=True)
        async def fetch_data(run: Run):
            data = await run.http.get_offset_paginated(os.path.join(run.config.base_url, "DataEndpoint"), headers=run.headers)
            df = DataFrame(data).collect()
            await run.db.write(df, 'mydatabase.data_table', primary_keys=['data_id'])
        
        @my_flow.task(main=True)
        async def fetch_processes(run: Run):
            data = await run.http.get_offset_paginated(os.path.join(run.config.base_url, "ProcessEndpoint"), headers=run.headers)
            processed_data = ProcessedData(data, infer_schema_length=None).collect()
            await run.db.write(processed_data, 'mydatabase.processed_data', primary_keys=['process_id'])

        if __name__ == "__main__":
            my_flow(db_url=DB, raise_all=True, http_per_second=100)
        ```
    """

    def __init__(
            self, 
            name: str,  
            config_model: Type[ConfigType] = Config,
            run_model: Type[RunType] = FlowRun
            ):
        self.name = name
        self.logger = Logger(self.name)
        self.config_model = config_model
        self.run_model = run_model
        self.tasks: List[TaskType] = []
        self._is_locked = False

    def __call__(self, *args, **kwargs):
        self.logger.warning(f"Flow called directly. This is blocking and not recommended for production use.")
        return asyncio.run(self.run(*args, **kwargs))

    def task(self, name: str = None, **kwargs: Unpack[TaskKwargs]):
        """
        Decorator to create a flow task with flexible behavior.

        Args:
            name (str, optional): The name of the task. If not provided, the function name will be used.
            run_first (bool, optional): If True, the task will be run at the beginning of the flow execution.
            run_last (bool, optional): If True, the task will be run at the end of the flow execution.
            main (bool, optional): If True, the task will be considered a main task and run during the main execution phase.
            interval_minutes (int, optional): If provided, the task will be run at the specified interval in minutes.

        Returns:
            Callable: The decorated function wrapped as a Task object and added to the flow's task list.
        """
        run_first = kwargs.get("run_first", False)
        depends_on = kwargs.get("depends_on")
        if run_first and depends_on:
            raise ValueError("Task cannot be both run_first and depends_on")
        def decorator(func: Callable[[RunType], Coroutine]):
            if asyncio.iscoroutinefunction(func):
                obj = AsyncTask(self, func, name=name, **kwargs)
            else:
                obj = SyncTask(self, func, name=name, **kwargs)
            self.tasks.append(obj)
            return obj
        return decorator

    def _main_coroutines(self, run: RunType):
        selected_tasks = [t for t in self.tasks if t.name in run.specified_tasks or t.fname in run.specified_tasks] if run.specified_tasks else self.tasks
        main_coroutines = [task.execute(run) for task in selected_tasks if task.main]
        return main_coroutines

    def _interval_coroutines(self, run: RunType):
        interval_tasks = [task for task in self.tasks if task.interval_minutes]
        interval_coroutines = [task.run_at_interval(run) for task in interval_tasks]
        return interval_coroutines

    def _dependent_coroutines(self, run: RunType):
        completed_runs = [tr.task for tr in run.task_runs if tr.status == Status.COMPLETED]
        dependent_coroutines = []
        for task in self.tasks:
            if task.depends_on and task not in run.task_runs and all([d in completed_runs for d in task.depends_on]):
                dependent_coroutines.append(task.execute(run))
        return dependent_coroutines

    async def _prepare_run(self, config: ConfigType = None, tasks: TaskIdentifier = None, **kwargs) -> FlowRun:
        specified_tasks = tasks or []
        config = config or self.config_model(**kwargs)
        run = self.run_model(
            flow=self.name, 
            flow_obj=self, 
            logger=self.logger, 
            config=config, 
            write_db=config.log_db, 
            kwargs=kwargs, 
            specified_tasks=specified_tasks
            )
        if config.log_db:
            await run.create_model_tables(self.run_model, TaskRun, HTTPRun, TransactionLog)
        await run.upsert_log(run)
        return run
    
    async def run(self, config: ConfigType = None, tasks: TaskIdentifier = None, **kwargs):
        """
        Executes the flow with the given configuration.
        This method runs the flow tasks in a specific order: first tasks, main tasks, and last tasks.
        It also handles interval tasks that need to be run at specific intervals. The flow can be locked
        to prevent concurrent executions.

        Args:
            config (type[Config], optional): Configuration class for the flow. If not provided, the default 
                                             configuration model will be used.
            tasks (List[TaskIdentifier], optional): An individual or list of main tasks to be executed as either string or the task object. If not provided, 
                                                    all tasks in the flow will be executed.
            **kwargs: Additional keyword arguments to be passed to the flow's config_model object. If config argument is provided, kwargs will be ignored.

        Returns:
            Run: An instance of the Run class representing the execution of the flow.

        Raises:
            ErrorThresholdExceeded: If the error threshold is exceeded during the execution of the flow.
            Exception: If any other error occurs during the execution of the flow, it will be raised after
                       updating the run status and error count.
        """
        run = await self._prepare_run(config, tasks, **kwargs)
        interval_task_group = asyncio.Future()
        async with run.http.get_session() as session:
            run.http._session = session
            try:
                await run.gather(*[task.execute(run) for task in self.tasks if task.run_first]) 
                interval_task_group = asyncio.gather(*self._interval_coroutines(run)) 
                await run.gather(*self._main_coroutines(run))
                while True:
                    coroutines = self._dependent_coroutines(run)
                    if not coroutines:
                        break
                    await run.gather(*coroutines)
                await run.gather(*[task.execute(run) for task in self.tasks if task.run_last]) 
                run.finish(Status.COMPLETED) if run.errors == 0 else run.finish(Status.PARTIAL)
            except Exception as e:
                run.finish(Status.FAILED, error_message=str(e))
                if not isinstance(e, ErrorThresholdExceeded):
                    run.error_increment()
                raise e
            finally:
                interval_task_group.cancel()
                try:
                    await interval_task_group 
                except asyncio.CancelledError:
                    pass
                if run.status == Status.RUNNING:
                    run.finish(Status.FAILED)
                await run.stop_log_worker()
                await run.upsert_log(run)
        return run


FlowRun.model_rebuild()
