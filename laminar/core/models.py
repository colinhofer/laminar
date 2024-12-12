from typing import Optional, Literal, TYPE_CHECKING
from pydantic import BaseModel, Field, ConfigDict
from datetime import datetime
from enum import StrEnum
import uuid

from ..logger.logger import Logger

class Status(StrEnum):
    UNKNOWN = "UNKNOWN"
    PARTIAL = "PARTIAL"
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

STATUS = Literal["RUNNING", "COMPLETED", "FAILED", "UNKNOWN", "PARTIAL"]

class LaminarBase(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))

class Log(LaminarBase):
    __tablename__: str = "test"
    log_type: str
    flow: str 
    start_time: datetime = Field(default_factory=datetime.now)
    end_time: datetime = None
    duration: float = None
    status: STATUS = Status.RUNNING
    error_message: Optional[str] = None
    finished: bool = Field(default=False, exclude=True)
    write_db: bool = Field(default=True, exclude=True)
    log_init: bool = Field(default=True, exclude=True)
    logger: Logger = Field(default=None, exclude=True)

    def model_post_init(self, __context):
        if self.log_init:
            self.log_state()
        return super().model_post_init(__context)

    def finish(self, status: Status, error_message = None):
        if not self.finished:
            self.end_time = datetime.now()
            self.duration = (self.end_time - self.start_time).total_seconds()
            self.status = status
            self.error_message = str(error_message) if error_message else None
            self.finished = True
            self.log_state()

    def log_state(self):
        self.logger.log_obj(self)
            
    def sql_upsert(self) -> tuple[str, list]:
        sql_insert, values = self.sql_insert()
        update_set_clause = ", ".join([f'"{field}" = EXCLUDED."{field}"' for field in self.model_dump(exclude_none=True).keys() if field != 'id'])
        sql_upsert = f"""
        {sql_insert.rstrip(';')}
        ON CONFLICT ("id") DO UPDATE
        SET {update_set_clause}
        """
        return sql_upsert, values

    def sql_insert(self) -> tuple[str, list]:
        table_name = self.__tablename__
        serialized_data = self.model_dump(exclude_none=True)
        columns = ", ".join([f'"{field}"' for field in serialized_data.keys()])
        placeholders = ", ".join([f"${i+1}" for i in range(len(serialized_data))])
        sql = f"""
        INSERT INTO {table_name} ({columns})
        VALUES ({placeholders})
        """
        return sql, list(serialized_data.values())
    
    def sqlite_upsert(self, with_schema: bool = True) -> tuple[str, list]:
        """Generates an SQL upsert (insert or update on conflict) for SQLite."""
        sql_insert, values = self.sqlite_insert(with_schema)
        update_set_clause = ", ".join([f'"{field}" = excluded."{field}"' for field in self.model_dump(exclude_none=True).keys() if field != 'id'])
        sql_upsert = f"""
        {sql_insert.rstrip(';')}
        ON CONFLICT ("id") DO UPDATE
        SET {update_set_clause}
        """
        return sql_upsert, values

    def sqlite_insert(self, with_schema: bool = True) -> tuple[str, list]:
        """Generates an SQL insert for SQLite."""
        table_name = self.__tablename__.split('.')[-1]
        serialized_data = self.model_dump(exclude_none=True)
        columns = ", ".join([f'"{field}"' for field in serialized_data.keys()])
        placeholders = ", ".join(["?" for _ in serialized_data])
        sql = f"""
        INSERT INTO {table_name} ({columns})
        VALUES ({placeholders})
        """
        return sql, list(serialized_data.values())

    @classmethod
    def sql_columns(cls) -> dict:
        columns = {}
        for field_name, field in cls.model_fields.items():
            if field.exclude:
                continue  
            column_type = "TEXT"
            if field.annotation == int:
                column_type = "INTEGER"
            elif field.annotation == float:
                column_type = "REAL"
            elif field.annotation == datetime:
                column_type = "TIMESTAMP"
            columns[field_name] = f'{column_type}{" PRIMARY KEY" if field_name == "id" else ""}'
        return columns