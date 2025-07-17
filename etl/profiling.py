import pandas as pd

def profile_dataframe(df: pd.DataFrame, sample_rows: int = 20) -> dict:
    profile = {}
    for col in df.columns:
        s = df[col]
        profile[col] = {
            "inferred_dtype": str(s.dtype),
            "null_pct": float(s.isna().mean() * 100),
            "num_unique": int(s.nunique(dropna=True)),
            "sample_values": s.dropna().astype(str).head(5).tolist(),
        }
    sample_csv = df.head(sample_rows).to_csv(index=False)
    return {"profile": profile, "sample_csv": sample_csv}

