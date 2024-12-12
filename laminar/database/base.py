from abc import ABC, abstractmethod
from typing import List, Optional, Tuple, List, Dict, Union, Any, Literal, get_args, AsyncGenerator,TYPE_CHECKING

from ..core.models import Log

if TYPE_CHECKING:
    from ..core.run import FlowRun

TRANSACTION = Literal["REPLACE", "APPEND", "UPSERT", None]

class TransactionLog(Log):
    __tablename__ = "logs.db_ops"
    run_id: str
    task: Optional[str] = None
    log_type: str = "DB"
    table_name: str
    rows: int = 0
    transaction: TRANSACTION = "UPSERT"
    message: str = None


class BaseDB(ABC):
    log_transaction_start: bool = False

    def __init__(self, run: Optional["FlowRun"] = None):
        self.run = run

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    @staticmethod
    def parse_table_name(full_table_name: str) -> Tuple[Optional[str], str]:
        parts = full_table_name.split('.')
        return (parts[0], parts[1]) if len(parts) == 2 else (None, parts[0])

    @abstractmethod
    async def connect(self):
        pass

    @abstractmethod
    async def close(self):
        pass

    @abstractmethod
    async def get_connection(self) -> AsyncGenerator:
        pass

    async def execute(self, sql: str, params: Any = None) -> None:
        async with self.get_connection() as conn:
            if params:
                await conn.execute(sql, params)
            else:
                await conn.execute(sql)

    async def query(self, sql: str, params: Any = None) -> List[Dict[str, Any]]:
        async with self.get_connection() as conn:
            cursor = await conn.execute(sql, params or [])
            rows = await cursor.fetchall()
            col_names = [description[0] for description in cursor.description]
            return [dict(zip(col_names, row)) for row in rows]

    async def create_table(self, table_name: str, schema: Dict[str, str], primary_keys: Optional[List[str]] = None):
        columns = [f'"{col}" {dtype}' + (" PRIMARY KEY" if primary_keys and col in primary_keys else "") for col, dtype in schema.items()]
        sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)});"
        await self.execute(sql)

    @abstractmethod
    async def table_exists(self, table_name: str) -> bool:
        pass
