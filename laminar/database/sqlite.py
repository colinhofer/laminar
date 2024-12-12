import aiosqlite
import os
import hashlib
from urllib.parse import urlparse
import asyncio
import polars as pl
from contextlib import asynccontextmanager
from datetime import datetime
from typing import List, Optional, Tuple, Dict, Union, Any, Literal, AsyncGenerator, TYPE_CHECKING

from .base import TransactionLog, BaseDB

if TYPE_CHECKING:
    from ..core.run import FlowRun
    from ..core.models import Log

TRANSACTION = Literal["REPLACE", "APPEND", "UPSERT", None]

class SQLite(BaseDB):
    log_transaction_start: bool = False

    def __init__(self, db_url: str, run: "FlowRun" = None):
        self.db_url = db_url
        self.run = run
        self.connection: Optional[aiosqlite.Connection] = None
        self.db_path = self._extract_db_path(db_url)

    async def connect(self):
        """Connects to the SQLite database, ensuring the file exists."""
        if not os.path.exists(self.db_path):
            open(self.db_path, 'a').close()
        if not self.connection:
            self.connection = await aiosqlite.connect(self.db_path)
            await self.connection.execute('PRAGMA foreign_keys = ON;')
            await self.connection.commit()

    async def close(self):
        """Closes the database connection."""
        if self.connection:
            await self.connection.close()
            self.connection = None

    def _extract_db_path(self, db_url: str) -> str:
        """Extracts the file path from a sqlite:/// URL, handling relative paths correctly."""
        parsed_url = urlparse(db_url)
        if parsed_url.scheme != 'sqlite':
            raise ValueError("Invalid database URL. Expected a sqlite:// URL.")
        db_path = parsed_url.path.lstrip('/')
        return db_path

    @asynccontextmanager
    async def get_connection(self) -> AsyncGenerator[aiosqlite.Connection, None]:
        connection = await aiosqlite.connect(self.db_path)
        try:
            await connection.execute('PRAGMA foreign_keys = ON;')
            await connection.commit()
            yield connection
        finally:
            await connection.close()

    async def execute(self, sql: str, params: Any = None) -> None:
        async with self.get_connection() as conn:
            await conn.execute(sql, params or [])
            await conn.commit()

    async def upsert_obj(self, obj: "Log"):
        if obj.write_db:
            await self.execute(*obj.sqlite_upsert())
        
    async def insert_obj(self, obj: "Log"):
        if obj.write_db:
            await self.execute(*obj.sqlite_insert())

    async def query(self, sql: str, params: Optional[Union[List, Dict]] = None) -> List[Dict[str, Any]]:
        async with self.get_connection() as conn:
            cursor = await conn.execute(sql, params or [])
            rows = await cursor.fetchall()
            col_names = [description[0] for description in cursor.description]
            return [dict(zip(col_names, row)) for row in rows]

    async def table_exists(self, table_name: str) -> bool:
        """Checks if a table exists."""
        sql = "SELECT name FROM sqlite_master WHERE type='table' AND name=?"
        result = await self.query(sql, [table_name])
        return bool(result)

    async def alter_model_table(self, table: str, sql_cols: dict) -> None:
        """Alters a table by adding new columns if they do not already exist."""
        table_data = await self.query(f"PRAGMA table_info({table});")
        existing_cols = {col['name'] for col in table_data}

        alter_statements = [
            f'ALTER TABLE {table} ADD COLUMN "{field_name}" {col_type}'
            for field_name, col_type in sql_cols.items()
            if field_name not in existing_cols
        ]
        for alter_sql in alter_statements:
            await self.execute(alter_sql)

    async def create_model_tables(self, *models: Tuple[type["Log"]]):
        """Creates or alters tables for multiple models."""
        tasks = [self._create_or_alter_model_table(m) for m in models]
        await self.run.gather(*tasks)

    async def _create_or_alter_model_table(self, model):
        """Creates a table or alters an existing table based on the model's SQL columns."""
        sql_cols = model.sql_columns()
        _, table = self.parse_table_name(model.__tablename__)
        exists = await self.table_exists(table)
        if not exists:
            cols = ", ".join([f'"{key}" {val}' for key, val in sql_cols.items()])
            create_sql = f'CREATE TABLE {table} ({cols});'
            await self.execute(create_sql)
        else:
            await self.alter_model_table(table, sql_cols)

    @staticmethod
    def get_sql_type(pl_dtype):
        """Maps Polars data types to SQLite-compatible SQL types."""
        type_mapping = {
            pl.Int8: "INTEGER",
            pl.Int16: "INTEGER",
            pl.Int32: "INTEGER",
            pl.Int64: "INTEGER",
            pl.UInt8: "INTEGER",
            pl.UInt16: "INTEGER",
            pl.UInt32: "INTEGER",
            pl.UInt64: "INTEGER",
            pl.Float32: "REAL",
            pl.Float64: "REAL",
            pl.Boolean: "INTEGER",
            pl.Utf8: "TEXT",
            pl.String: "TEXT",
            pl.Binary: "BLOB",
            pl.Date: "DATE",
            pl.Datetime: "DATETIME",
            pl.Time: "TIME",
            pl.List: "TEXT",  
            pl.Struct: "TEXT",  
            pl.Object: "TEXT"  
        }
        return type_mapping.get(pl_dtype, "TEXT") 
    
    @classmethod
    def generate_temp_table_name(cls, table_name) -> str:
        _, table = cls.parse_table_name(table_name)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        hash_suffix = hashlib.md5(f"{table}{timestamp}".encode()).hexdigest()[:8]
        return f"temp_{table}_{hash_suffix}"

    async def table_exists(self, table_name: str) -> bool:
        sql = """
        SELECT name FROM sqlite_master WHERE type='table' AND name=?
        """
        result = await self.query(sql, [table_name])
        return bool(result)

    async def create_table(self, table_name: str, df: pl.DataFrame, primary_keys: Optional[List[str]] = None) -> None:
        """Creates a new table in the SQLite database based on the Polars DataFrame schema."""
        _, table_name = self.parse_table_name(table_name)
        columns = []
        for col_name, dtype in df.schema.items():
            sql_type = self.get_sql_type(dtype)
            is_pk = primary_keys and col_name in primary_keys
            nullable = "NOT NULL" if is_pk else "NULL"
            columns.append(f'"{col_name}" {sql_type} {nullable}')
        if primary_keys:
            pk_columns = ', '.join(f'"{pk}"' for pk in primary_keys)
            columns.append(f"PRIMARY KEY ({pk_columns})")
        columns_sql = ',\n    '.join(columns)
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {columns_sql}
        )
        """
        await self.execute(create_table_sql)

    async def write(self, df: pl.DataFrame, table_name: str, if_exists: str = "UPSERT",
                    chunk_size: int = 10000, primary_keys: Optional[List[str]] = None) -> None:
        
        _, table_name = self.parse_table_name(table_name)  # SQLite does not support schemas
        log = TransactionLog(
            flow=self.run.flow,
            logger=self.run.logger,
            run_id=self.run.id,
            table_name=table_name,
            transaction=if_exists,
            rows=len(df),
            log_init=self.log_transaction_start
        )
        if primary_keys:
            df = df.drop_nulls(primary_keys).unique(primary_keys)
        if not log.rows:
            log.finish("UNKNOWN", "Empty dataframe")
            log.log()
            return

        if if_exists not in ["REPLACE", "APPEND", "UPSERT"]:
            raise ValueError(f"if_exists must be one of 'REPLACE', 'APPEND', 'UPSERT'")

        if if_exists == 'UPSERT' and not primary_keys:
            raise ValueError("primary_keys required for upsert mode")

        if primary_keys and not all(pk in df.columns for pk in primary_keys):
            missing_keys = set(primary_keys) - set(df.columns)
            raise ValueError(f"Primary keys not found in DataFrame: {missing_keys}")
        
        try:
            table_exists = await self.table_exists(table_name)
            if table_exists:
                if if_exists == 'REPLACE':
                    await self.drop_table(table_name)
                    await self.create_table(table_name, df, primary_keys)
                elif if_exists == 'UPSERT':
                    temp_table = self.generate_temp_table_name(table_name)
                    await self.create_table(temp_table, df, primary_keys)
                    for i in range(0, df.height, chunk_size):
                        chunk = df.slice(i, chunk_size)
                        await self.insert(chunk, temp_table)
                    await self.upsert(table_name, temp_table, df.columns, primary_keys)
                    await self.drop_table(temp_table)
            else:
                await self.create_table(table_name, df, primary_keys)
                for i in range(0, df.height, chunk_size):
                    chunk = df.slice(i, chunk_size)
                    await self.insert(chunk, table_name)
            log.finish("COMPLETED")
            await self.execute(*log.sqlite_insert())

        except Exception as e:
            log.finish("FAILED", str(e))
            await self.execute(*log.sqlite_insert())
            raise e

    async def drop_table(self, table: str):
        await self.execute(f"DROP TABLE IF EXISTS {table}")

    async def insert(self, df: pl.DataFrame, table: str):
        """Inserts data from a Polars DataFrame into the specified SQLite table."""
        columns = df.columns
        values = [tuple(row) for row in df.rows()]
        if not values:
            return
        column_names = ', '.join(f'"{col}"' for col in columns)
        placeholders = ', '.join('?' for _ in columns)
        insert_query = f"INSERT INTO {table} ({column_names}) VALUES ({placeholders})"
        for value_set in values:
            await self.execute(insert_query, value_set)

    async def upsert(self, destination: str, source: str, columns: List[str], primary_keys: List[str]):
        """Performs an upsert (insert or update on conflict) from the source table to the destination table."""
        non_pk_columns = [col for col in columns if col not in primary_keys]
        pk_quoted = [f'"{pk}"' for pk in primary_keys]
        update_stmt = ", ".join(f'"{col}" = excluded."{col}"' for col in non_pk_columns)
        insert_query = f"""
            INSERT INTO {destination} ({", ".join(columns)})
            SELECT {", ".join(columns)} FROM {source} WHERE true
            ON CONFLICT({", ".join(pk_quoted)})
            DO UPDATE SET {update_stmt};
        """
        await self.execute(insert_query)

