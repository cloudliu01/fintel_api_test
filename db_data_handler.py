import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

from sqlalchemy import create_engine
from sqlalchemy import Table, Column, MetaData, text
from sqlalchemy import String, Float, Integer, Boolean, DateTime, BigInteger, Numeric

from sqlalchemy.engine import Engine
from typing import Optional, List
from sqlalchemy import create_engine, inspect
from sqlalchemy.exc import OperationalError

from numpy import dtype

TABLE_STOCK_SCHEMA = {  
}

class DBDataHandler:
    def __init__(self, db_name: str, db_user: str, db_password: str, db_host: str, db_port: int, 
                         unique_cols:list =['symbol', 'slug', 'exchange', 'formtype', 'filedate', 'effectivedate', 'shares', 'shareschange', 'value']):
        """
        Initialize the handler with a PostgreSQL SQLAlchemy DB URL.
        Example: 'postgresql+psycopg2://user:password@localhost:5432/mydb'
        """
        self.engine: Engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
        self.db_name = db_name
        self.user = db_user 
        self.password = db_password
        self.host = db_host
        self.port = db_port
        self.unique_cols = unique_cols
        self.db_url = f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
        self._ensure_database_exists()

    def _map_dtype(self, dtype):
        if pd.api.types.is_bool_dtype(dtype):
            return Boolean()
        elif pd.api.types.is_integer_dtype(dtype):
            return Integer()
        elif pd.api.types.is_float_dtype(dtype):
            return Float()
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            return DateTime()
        elif pd.api.types.is_numeric_dtype(dtype):
            return Numeric()
        else:
            return String()

    def _ensure_database_exists(self ):
        """
        Ensure that the target database exists; if not, create it.

        This connects to the 'postgres' default DB to create 'db_name'.
        """
        try:
            conn = psycopg2.connect(
                dbname='postgres', # !!! Connect to the default 'postgres' database to create new database
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (self.db_name,))
                exists = cur.fetchone()
                if not exists:
                    print(f"Database '{self.db_name}' does not exist. Creating it...")
                    cur.execute(f'CREATE DATABASE "{self.db_name}"')
                else:
                    print(f"Database '{self.db_name}' already exists.")
            conn.close()
        except Exception as e:
            print(f"[ERROR] Failed to ensure database exists: {e}")
            raise


    def initialize_table(self, df, table_name: str):
        """
        Explicitly initialize (create) the table based on a provided schema.

        :param table_name: Name of the target table
        :param schema: Dict of column_name: numpy/pandas dtype
        """
        metadata = MetaData()
        columns = []

        type_map = {
            "bool": Boolean,
            "int64": BigInteger,
            "float64": Float,
            "datetime64[ns]": DateTime,
            "O": String,  # object type
        }

        for col_name, dtype in df.dtypes.to_dict().items():
            dtype_str = str(dtype)
            col_type = type_map.get(dtype_str, String)
            columns.append(Column(col_name, col_type))

        table = Table(table_name, metadata, *columns)
        metadata.create_all(self.engine)

        self.create_unique_index_if_not_exists(table_name)


    def create_unique_index_if_not_exists(self, table_name):
        unique_str = ", ".join(f'"{col}"' for col in self.unique_cols)
        index_name = f"{table_name}_uniq_idx"

        create_index_sql = f'''
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_class c
                WHERE c.relname = '{index_name}'
            ) THEN
                EXECUTE 'CREATE UNIQUE INDEX {index_name} ON "{table_name}" ({unique_str})';
            END IF;
        END$$;
        '''

        with self.engine.connect() as conn:
            conn.execute(text(create_index_sql))
            conn.commit()
            print(f"✅ Unique index '{index_name}' created on ({unique_str}) if it didn't exist.")


    def fetch_table(self, table_name: str) -> pd.DataFrame:
        """Fetch an entire table from the database as a DataFrame."""
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, self.engine)
        print(f"Fetched {len(df)} rows from '{table_name}'.")
        return df


    def upload_dataframe(self, df, table_name):
        # Step 1: Connect to the DB
        conn = psycopg2.connect(
            dbname=self.db_name,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port
        )
        cur = conn.cursor()
    
        # Step 2: Check if table exists
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = %s
            )
        """, (table_name,))
    
        # Step 3: Insert with ON CONFLICT DO NOTHING
        columns = list(df.columns)
        values = df.values.tolist()
        col_names = ", ".join(f'"{c}"' for c in columns)
        conflict_str = ", ".join(f'"{c}"' for c in self.unique_cols)
    
        insert_query = f"""
            INSERT INTO "{table_name}" ({col_names})
            VALUES %s
            ON CONFLICT ({conflict_str}) DO NOTHING
        """
    
        execute_values(cur, insert_query, values)
        conn.commit()
        cur.close()
        conn.close()
        print(f"Inserted {len(values)} rows into '{table_name}' (conflicts skipped).")
