
import requests
import pandas as pd

def extract_from_csv(file_path: str) -> pd.DataFrame:
    return pd.read_csv(file_path)

def extract_from_api(api_url: str) -> pd.DataFrame:
    res = requests.get(api_url)
    return pd.DataFrame(res.json())

def extract_from_sql(engine, query: str) -> pd.DataFrame:
    return pd.read_sql(query, engine)