import asyncpg
import hashlib
import asyncio
import polars as pl
from contextlib import asynccontextmanager
from datetime import datetime
from typing import List, Optional, Tuple, List, Dict, Union, Any, Literal, get_args, AsyncGenerator, TYPE_CHECKING

from .base import TransactionLog, BaseDB, TRANSACTION

if TYPE_CHECKING:
    from ..core.run import FlowRun
    from ..core.models import Log

class Postgres(BaseDB):
    log_transaction_start: bool = False
    
    def __init__(self, url: str = None, run: "FlowRun" = None, min_connections: int = 1, max_connections: int = 10):
        self.url = url
        self.min_connections = min_connections
        self.max_connections = max_connections
        self.run = run
        self.pool: Optional[asyncpg.Pool] = None

    async def connect(self):
        if not self.pool:
            self.pool = await asyncpg.create_pool(
                self.url,
                min_size=self.min_connections,
                max_size=self.max_connections
            )

    async def close(self):
        if self.pool:
            await self.pool.close()
            self.pool = None

    @asynccontextmanager
    async def get_connection(self) -> AsyncGenerator[asyncpg.Connection, None]:
        if not self.pool:
            await self.connect()
        async with self.pool.acquire() as conn:
            yield conn

    async def execute(self, sql: str, params: Any = None) -> str:
        async with self.get_connection() as conn:
            return await conn.execute(sql, *params) if params else await conn.execute(sql)
        
    async def upsert_obj(self, obj: "Log"):
        if obj.write_db:
            await self.execute(*obj.sql_upsert())
        
    async def insert_obj(self, obj: "Log"):
        if obj.write_db:
            await self.execute(*obj.sql_insert())

    async def query(self, 
                    sql: str, 
                    params: Optional[Union[List, Dict]] = None, 
                    as_polars: bool = False,
                    raw_response: bool = False
                   ) -> Union[List[Dict[str, Any]], pl.DataFrame]:
        async with self.get_connection() as conn:
            if isinstance(params, dict):
                results = await conn.fetch(sql, **params)
            elif isinstance(params, list):
                results = await conn.fetch(sql, *params)
            else:
                results = await conn.fetch(sql)
            if raw_response:
                return results
            data = [dict(row) for row in results]
            return pl.DataFrame(data) if as_polars else data
        
    async def table(self, table_name: str, as_polars: bool = False) -> Union[List[Dict[str, Any]], pl.DataFrame]:
        sql = f"SELECT * FROM {table_name}"
        return await self.query(sql, as_polars=as_polars)
        

    @staticmethod
    def get_sql_type(pl_dtype):
        type_mapping = {
            pl.Int8: "SMALLINT",
            pl.Int16: "SMALLINT",
            pl.Int32: "INTEGER",
            pl.Int64: "BIGINT",
            pl.UInt8: "SMALLINT",
            pl.UInt16: "INTEGER",
            pl.UInt32: "BIGINT",
            pl.UInt64: "NUMERIC(20)",
            pl.Float32: "REAL",
            pl.Float64: "DOUBLE PRECISION",
            pl.Decimal: "NUMERIC",
            pl.Boolean: "BOOLEAN",
            pl.Utf8: "TEXT",
            pl.String: "TEXT",
            pl.Binary: "BYTEA",
            pl.Date: "DATE",
            pl.Datetime: "TIMESTAMP",
            pl.Time: "TIME",
            pl.Duration: "INTERVAL",
            pl.Categorical: "TEXT",
            pl.Enum: "INTEGER",
            pl.List: "ARRAY",
            pl.Struct: "JSONB",
            pl.Null: "TEXT",
            pl.Object: "JSONB"
        }
        return type_mapping.get(pl_dtype, "TEXT")

    @classmethod
    def generate_temp_table_name(cls, table_name) -> str:
        schema, table = cls.parse_table_name(table_name)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        hash_suffix = hashlib.md5(f"{table}{timestamp}".encode()).hexdigest()[:8]
        return f"{schema+'.' if schema else ''}temp_{table}_{hash_suffix}"

    async def table_exists(self, schema: str, table_name: str) -> bool:
        sql = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = $1 AND table_name = $2
        )
        """
        result = await self.query(sql, [schema, table_name])
        return result[0]['exists']

    async def create_table(self, table_name: str, df: pl.DataFrame, primary_keys: Optional[List[str]] = None) -> None:
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
        CREATE TABLE {table_name} (
            {columns_sql}
        )
        """
        await self.execute(create_table_sql)

    async def insert(self, df: pl.DataFrame, table: str):
        async with self.get_connection() as conn:
            columns = df.columns
            values = [tuple(row) for row in df.rows()]
            column_names = ','.join(f'"{col}"' for col in columns)
            placeholders = ','.join(f'${i+1}' for i in range(len(columns)))
            insert_query = f"INSERT INTO {table} ({column_names}) VALUES ({placeholders})"
            await conn.executemany(insert_query, values)

    async def drop_table(self, table: str):
        await self.execute(f"DROP TABLE IF EXISTS {table}")

    async def upsert(self, destination: str, source: str, columns: list, primary_keys: list):
        pk_conditions = " AND ".join([f"excluded.{pk} = {destination}.{pk}" for pk in primary_keys])
        non_pk_columns = [col for col in columns if col not in primary_keys]
        update_stmt = ", ".join(f"{col} = excluded.{col}" for col in non_pk_columns)
        upsert_query = f"""
            INSERT INTO {destination}
            SELECT * FROM {source}
            ON CONFLICT ({', '.join(primary_keys)})
            DO UPDATE SET {update_stmt}
            WHERE {pk_conditions}
        """
        await self.execute(upsert_query)

    async def create_schema(self, schema: str):
        await self.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    async def write(self, df: pl.DataFrame, table_name: str, if_exists: TRANSACTION = "UPSERT",
                    chunk_size: int = 10000, primary_keys: Optional[List[str]] = None) -> None:
        
        log = TransactionLog(flow=self.run.flow, logger=self.run.logger, run_id=self.run.id, table_name=table_name, transaction=if_exists, rows=len(df), log_init=self.log_transaction_start)
        df = df.drop_nulls(primary_keys).unique(primary_keys) if primary_keys else df
        if not log.rows:
            log.finish("UNKNOWN", "Empty dataframe")
            log.log()
            return
        if if_exists not in get_args(TRANSACTION):
            raise ValueError(f"if_exists must be one of {get_args(TRANSACTION)}")

        if if_exists == 'UPSERT' and not primary_keys:
            raise ValueError("primary_keys required for upsert mode")

        if primary_keys and not all(pk in df.columns for pk in primary_keys):
            missing_keys = set(primary_keys) - set(df.columns)
            raise ValueError(f"Primary keys not found in DataFrame: {missing_keys}")


        schema, base_table_name = self.parse_table_name(table_name)
        full_table_name = f"{schema+'.' if schema else ''}{base_table_name}"

        try:
            if schema:
                await self.create_schema(schema)
            if await self.table_exists(schema, base_table_name):
                if if_exists is None:
                    raise ValueError(f"Table {full_table_name} already exists - change if_exists parameter to perform operations on it")
                elif if_exists == 'REPLACE':
                    await self.drop_table(full_table_name)
                    await self.create_table(full_table_name, df, primary_keys)
                elif if_exists == 'UPSERT':
                    temp_table = self.generate_temp_table_name(full_table_name)
                    await self.create_table(temp_table, df)
                    for i in range(0, df.height, chunk_size):
                        chunk = df.slice(i, chunk_size)
                        await self.insert(chunk, temp_table)
                    await self.upsert(full_table_name, temp_table, df.columns, primary_keys)
                    await self.drop_table(temp_table)
            else:
                await self.create_table(full_table_name, df, primary_keys)
                for i in range(0, df.height, chunk_size):
                    chunk = df.slice(i, chunk_size)
                    await self.insert(chunk, full_table_name)

            log.finish("COMPLETED")
            await self.execute(*log.sql_insert())

        except Exception as e:
            log.finish("FAILED", e)
            await self.execute(*log.sql_insert())
            raise

    async def alter_model_table(self, schema: str, table: str, sql_cols: dict) -> None:
        table_data = await self.query(f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = '{schema}'
            AND table_name = '{table}';
        """)
        existing_cols = {col['column_name'] for col in table_data}
        alter_statements = [
            f'ADD COLUMN "{field_name}" {col_type}'
            for field_name, col_type in sql_cols.items()
            if field_name not in existing_cols
        ]
        if alter_statements:
            alter_table_sql = f'ALTER TABLE {schema}.{table} {", ".join(alter_statements)};'
            await self.execute(alter_table_sql)

    async def create_model_tables(self, *models: Tuple[type["Log"]]):
        tasks = []
        for m in models:
            tasks.append(self._create_or_alter_model_table(m))
        await asyncio.gather(*tasks)

    async def _create_or_alter_model_table(self, model):
        sql_cols = model.sql_columns()
        schema, table = self.parse_table_name(model.__tablename__)
        if schema:
            await self.create_schema(schema)
        exists = await self.table_exists(schema, table)
        if not exists:
            cols = ", ".join([f'"{key}" {val}' for key, val in sql_cols.items()])
            create_sql = f'CREATE TABLE {model.__tablename__} ({cols});'
            await self.execute(create_sql)
        else:
            await self.alter_model_table(schema, table, sql_cols)
