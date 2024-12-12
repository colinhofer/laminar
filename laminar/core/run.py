from pydantic import Field, ConfigDict
from typing import TYPE_CHECKING, Optional, List, Any, TypeVar, Generic, Union, Tuple, Dict
from datetime import datetime
import asyncio
from .exceptions import ErrorThresholdExceeded, FlowAborted, TaskTimeout, TaskDependencyError
from .models import LaminarBase, Log, Status
from ..database import Postgres, SQLite
from ..logger import Logger
from ..http import HTTPClient

if TYPE_CHECKING:
    from .flow import Flow, TaskIdentifier
    from .task import TaskRun, TaskType

DEFAULT_DB = "postgres://colin:1234@localhost:5432/piper"

def get_default_flow():
    from . import DEFAULT_FLOW
    return DEFAULT_FLOW

class Config(LaminarBase):
    """
    Config is a Pydantic model for configuring the flow run.
    Attributes:
        db_url (str): The database URL. Defaults to DEFAULT_DB.
        http_concurrency_limit (int): The limit for HTTP concurrency. Defaults to 10000.
        http_per_second (int): The limit for HTTP requests per second. Defaults to 10000.
        raise_errors (bool): Flag to indicate whether to raise exceptions. If False they will be caught and logged where possible. Defaults to False.
        lock_flow (bool): Flag to indicate whether to lock the flow during runs to prevent concurrent runs. Defaults to False.
        log_db (bool): Flag to indicate whether to log to the database. Defaults to True.
        tasks (List[str]): A list of main tasks to run. If None, all main tasks will run. Defaults to None.

        username (str): A generic username attribute for use in tasks. Defaults to None.
        api_key (str): A generic API key attribute for use in tasks. Defaults to None.
        password (str): A generic password attribute for use in tasks. Defaults to None.
        base_url (str): A generic base url attribute for use in tasks. Defaults to None.
    Subclasses can be created to add more attributes that need to be passed to the run.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True, validate_assignment=True)
    db_url: str = Field(default=None)
    created_at: datetime = Field(default_factory=datetime.now)
    http_concurrency_limit: int = 10000
    http_per_second: int = 10000
    http_retry_backoff: int = 1
    http_retries: int = 0
    error_limit: int = None
    raise_all: bool = False
    lock_flow: bool = False
    log_db: bool = True
    tasks: List[str] = []

    username: str = None
    api_key: str = None
    password: str = None
    base_url: str = None
    client_id: str = None
    client_secret: str = None

    def model_post_init(self, __context):
        if not self.db_url:
            self.log_db = False
        return super().model_post_init(__context)

ConfigType = TypeVar("ConfigType", bound=Config)

class FlowRun(Log, Generic[ConfigType]):
    __tablename__ = "logs.flow_run"
    log_type: str = "MAIN"
    errors: int = 0
    _task_queue: list = []
    _log_worker_task: asyncio.Task = None
    token: str = Field(default=None, exclude=True)
    headers: dict = Field(default=None, exclude=True)
    cookies: dict = Field(default=None, exclude=True)
    config: ConfigType = Field(default_factory=Config, exclude=True)
    db: Optional[Union["Postgres", "SQLite"]] = Field(default=None, exclude=True)
    http: Optional["HTTPClient"] = Field(default=None, exclude=True)
    task_runs: List["TaskRun"] = Field(default=[], exclude=True)
    log_queue: asyncio.Queue = Field(default=None, exclude=True)
    flow_obj: "Flow" = Field(default=get_default_flow, exclude=True)
    specified_tasks: List[Any] = Field(default=[], exclude=True)

    def model_post_init(self, __context):
        if self.logger is None:
            self.logger = Logger(self.flow)
        if self.http is None:
            self.http = HTTPClient(self)
        if self.config.db_url:
            if "sqlite" in self.config.db_url:
                self.db = SQLite(self.config.db_url, self)
            else:
                self.db = Postgres(self.config.db_url, self)
        if self.config.log_db:
            self.log_queue = asyncio.Queue()
            self._log_worker_task = asyncio.create_task(self.log_worker())
        return super().model_post_init(__context)

    async def log_worker(self):
        while True:
            obj = await self.log_queue.get()
            await self.upsert_log(obj)
            self.log_queue.task_done()

    async def stop_log_worker(self):
        if self.log_queue is not None:
            await self.log_queue.join()
            self._log_worker_task.cancel()
            try:
                await self._log_worker_task
            except asyncio.CancelledError:
                pass

    async def sleep(self, seconds: int):
        await asyncio.sleep(seconds)

    async def has_run(self, *tasks: Tuple["TaskIdentifier"]):
        return all(t in self.task_runs for t in tasks)

    async def wait_for(self, *tasks: Tuple["TaskIdentifier"], timeout: float = 86400):
        if not all(t in self.flow_obj.tasks for t in tasks):
            raise TaskDependencyError("Waiting for tasks that are not in the flow")
        async def wait_for_tasks():
            while not await self.has_run(*tasks):
                await self.sleep(0.5)
        try:
            await asyncio.wait_for(wait_for_tasks(), timeout=timeout)
        except asyncio.TimeoutError:
            raise TaskTimeout(
                f"Timeout reached: Tasks {tasks} did not all complete within {timeout} seconds."
            )

    def abort(self):
        """Raise a FlowAborted exception to stop the flow."""
        raise FlowAborted(f"Flow {self.flow} was aborted")

    def error_increment(self):
        self.errors += 1
        if self.config.error_limit and self.errors > self.config.error_limit:
            raise ErrorThresholdExceeded(f"Exceeded allowable error threshold")

    async def gather(self, *tasks):
        gathered_tasks = asyncio.gather(*tasks)
        self._task_queue.append(gathered_tasks)
        return await gathered_tasks

    async def create_model_tables(self, *models):
        if self.config.log_db:
            await self.db.create_model_tables(*models)

    async def upsert_log(self, obj: Log):
        if self.config.log_db:
            await self.db.upsert_obj(obj)

    async def insert_log(self, obj: Log):
        if self.config.log_db:
            await self.db.insert_obj(obj)


RunType = TypeVar("RunType", bound=FlowRun)
