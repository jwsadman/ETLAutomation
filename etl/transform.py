
from etl.llm_agent import query_groq
import pandas as pd

def transform_with_llm(df: pd.DataFrame) -> pd.DataFrame:
    prompt = f"""
    You are a senior data engineer.
    Clean the following table, handle missing values,remove duplicates,fix data types, add obvious derived features, and suggest fixes.

    DataFrame:
    {df.head(5).to_csv(index=False)}

    Respond with cleaned column names and transformations in CSV format.
    """
    response = query_groq(prompt)
    from io import StringIO
    return pd.read_csv(StringIO(response))
