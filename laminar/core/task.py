from typing import Callable, Coroutine, Optional, Union, Any, TypedDict, Unpack, TYPE_CHECKING
from datetime import datetime
from datetime import datetime
import inspect
import asyncio
from .run import FlowRun, RunType, get_default_flow
from .exceptions import FlowException
from .models import Log, Status
from ..logger import Logger

if TYPE_CHECKING:
    from .flow import Flow

class TaskKwargs(TypedDict):
    run_first: bool
    run_last: bool
    depends_on: Union[str, list]
    main: bool
    interval_minutes: int
    flow: "Flow"

def task(
    name: str = None,
    flow: "Flow" = None,
    **kwargs: Unpack[TaskKwargs]
):
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
    depends_on = kwargs.get("depends_on", None)
    if run_first and depends_on:
        raise ValueError("Task cannot be both run_first and depends_on")

    def decorator(func: Callable[[RunType], Coroutine]):
        if asyncio.iscoroutinefunction(func):
            obj = AsyncTask(
                func=func,
                name=name,
                flow=flow,
                **kwargs
            )
        else:
            obj = SyncTask(
                func=func,
                name=name,
                flow=flow,
                **kwargs
            )
        return obj

    return decorator


class TaskRun(Log):
    __tablename__ = "logs.task_run"
    task: Optional[str] = None
    log_type: str = "TASK"
    run_id: str

    def __eq__(self, other):
        if isinstance(other, (str, TaskType)):
            return other == self.task
        return other == self.task


class BaseTask:
    def __init__(
        self,
        flow: "Flow",
        func: Callable[..., Coroutine],
        name: Optional[str] = None,
        run_first: bool = False,
        run_last: bool = False,
        depends_on: Optional[Union[str, list, Callable]] = None,
        interval_minutes: Optional[int] = None,
        main: bool = False,
    ):
        self.func = func
        self.flow = flow or get_default_flow()
        self.name = name or func.__name__
        self.logger = flow.logger if flow else Logger()
        self.fname = func.__name__
        self.run_first = run_first
        self.run_last = run_last
        self.depends_on = depends_on if isinstance(depends_on, list) else [depends_on]
        self.interval_minutes = interval_minutes
        self.main = main
        self.last_run: Optional[datetime] = None
        sig = inspect.signature(self.func)
        self.expects_run = "run" in sig.parameters

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        if isinstance(other, str):
            return other == self.name or other == self.fname
        elif callable(other):
            return other.__name__ == self.name and other.__name__ == self.fname
        elif isinstance(other, TaskRun):
            return other.task == self.name
        return False

    def _parse_args(
        self, *args: Any, **kwargs: Any
    ) -> tuple[Optional[RunType], tuple, dict]:
        if args and isinstance(args[0], self.flow.run_model):
            run = args[0]
            if not self.expects_run:
                args = args[1:]
        elif "run" in kwargs and isinstance(kwargs["run"], self.flow.run_model):
            run = kwargs["run"]
            if not self.expects_run:
                kwargs.pop("run")
        else:
            self.logger.debug(
                f"Task {self.name} is missing a Run object, using flow.default_run"
            )
            run = self.flow.run_model(
                log_init=False, config=self.flow.config_model(), flow=self.name
            )
        return run, args, kwargs

    async def execute(self, *args, **kwargs):
        raise NotImplementedError

    async def run_at_interval(self, run: FlowRun):
        while True:
            if self.interval_minutes:
                await asyncio.sleep(self.interval_minutes * 60)
                await self.execute(run)


class AsyncTask(BaseTask):

    async def execute(self, *args, **kwargs):
        return await self(*args, **kwargs)

    async def __call__(self, *args, **kwargs):
        run, args, kwargs = self._parse_args(*args, **kwargs)
        task_run = TaskRun(
            run_id=run.id,
            flow=run.flow,
            logger=self.logger,
            task=self.name,
            write_db=run.config.log_db,
        )
        await run.upsert_log(task_run)
        try:
            result = await self.func(*args, **kwargs)
            task_run.finish(Status.COMPLETED)
            self.last_run = task_run.end_time

            return result
        except Exception as e:
            task_run.finish(Status.FAILED, e)
            run.error_increment()
            if isinstance(e, FlowException) or run.config.raise_all:
                raise e
        finally:
            if not task_run.finished:
                task_run.finish(Status.FAILED)
            run.task_runs.append(task_run)
            await run.upsert_log(task_run)


class SyncTask(BaseTask):

    async def execute(self, *args, **kwargs):
        return self(*args, **kwargs)

    def __call__(self, *args, **kwargs):
        run, args, kwargs = self._parse_args(*args, **kwargs)
        task_run = TaskRun(
            run_id=run.id,
            logger=self.logger,
            flow=run.flow,
            task=self.name,
            write_db=run.config.log_db,
        )
        if run.config.log_db and run.log_queue is not None:
            run.log_queue.put_nowait(task_run)
        try:
            result = self.func(*args, **kwargs)
            task_run.finish("COMPLETED")
            self.last_run = task_run.end_time
            return result
        except Exception as e:
            task_run.finish("FAILED", e)
            run.error_increment()
            if isinstance(e, FlowException) or run.config.raise_all:
                raise e
        finally:
            if not task_run.finished:
                task_run.finish("FAILED")
            run.task_runs.append(task_run)
            if run.config.log_db and run.log_queue is not None:
                run.log_queue.put_nowait(task_run)


TaskType = Union[SyncTask, AsyncTask]
