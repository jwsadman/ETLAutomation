from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
import psycopg2
import pandas as pd
from urllib.parse import urlparse

def ensure_database_exists(conn_str):
    """
    Ensures the target PostgreSQL database exists.
    If not, it will create the database.
    """
    parsed = urlparse(conn_str)
    db_name = parsed.path.lstrip("/")
    user = parsed.username
    password = parsed.password
    host = parsed.hostname
    port = parsed.port or 5432

    # Connect to default postgres database
    default_conn = psycopg2.connect(
        dbname="postgres",
        user=user,
        password=password,
        host=host,
        port=port
    )
    default_conn.autocommit = True
    cur = default_conn.cursor()

    # Check if DB exists
    cur.execute("SELECT 1 FROM pg_database WHERE datname=%s", (db_name,))
    exists = cur.fetchone()

    if not exists:
        print(f"[INFO] Database '{db_name}' does not exist. Creating...")
        cur.execute(f"CREATE DATABASE {db_name}")
        print(f"[INFO] Database '{db_name}' created successfully.")
    else:
        print(f"[INFO] Database '{db_name}' already exists.")

    cur.close()
    default_conn.close()


def load_to_postgres(df: pd.DataFrame, table_name: str, conn_str: str, if_exists="replace", chunksize=1000):
    """
    Load a DataFrame into PostgreSQL table. Creates DB if missing.
    """
    # Ensure DB exists
    ensure_database_exists(conn_str)

    try:
        engine = create_engine(conn_str)
        df.to_sql(table_name, engine, if_exists=if_exists, index=False, chunksize=chunksize)
        print(f"[INFO] Successfully loaded data into table '{table_name}'.")
    except OperationalError as e:
        print("[ERROR] Failed to connect or insert data:", e)
        raise

