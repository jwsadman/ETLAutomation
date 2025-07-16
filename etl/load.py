import pandas as pd
from sqlalchemy import create_engine

def load_to_postgres(df: pd.DataFrame, table_name: str, conn_str: str):
    engine = create_engine(conn_str)
    df.to_sql(table_name, engine, if_exists='replace', index=False)
