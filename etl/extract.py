import pandas as pd

def extract_from_csv(path: str, on_bad_lines: str = "skip", **kwargs) -> pd.DataFrame:
    df = pd.read_csv(path, on_bad_lines=on_bad_lines, **kwargs)
    print(f"[Extract] Loaded {len(df)} rows from {path}")
    return df

