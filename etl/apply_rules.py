
import pandas as pd
from typing import Any, Dict

_DTYPE_MAP = {
    "int": "Int64",
    "float": "float64",
    "string": "string",
    "bool": "boolean",
    "date": "datetime64[ns]",
    "datetime": "datetime64[ns]"
}

def _apply_fillna(df, col, strat):
    if strat is None:
        return df
    s = df[col]
    if isinstance(strat, (int, float, str, bool)):
        df[col] = s.fillna(strat)
        return df
    strat = str(strat).lower()
    if strat == "mean" and pd.api.types.is_numeric_dtype(s):
        df[col] = s.fillna(s.mean())
    elif strat == "median" and pd.api.types.is_numeric_dtype(s):
        df[col] = s.fillna(s.median())
    elif strat == "mode":
        m = s.mode(dropna=True)
        if not m.empty:
            df[col] = s.fillna(m.iloc[0])
    elif strat == "ffill":
        df[col] = s.ffill()
    elif strat == "bfill":
        df[col] = s.bfill()
    elif strat == "drop":
        df = df.dropna(subset=[col])
    return df

def _cast_dtype(df, col, label):
    if not label:
        return df
    label = label.lower()
    if label in ("date","datetime"):
        df[col] = pd.to_datetime(df[col], errors="coerce")
        return df
    if label == "bool":
        df[col] = df[col].astype("boolean", errors="ignore")
        return df
    target = _DTYPE_MAP.get(label)
    if target:
        try:
            df[col] = df[col].astype(target)
        except Exception:
            pass
    return df

def apply_llm_rules(df: pd.DataFrame, rules: Dict[str, Any]) -> pd.DataFrame:
    out = df.copy()

    # columns
    col_rules = rules.get("columns", {})
    renames, dtypes, fills = {}, {}, {}
    for old, spec in col_rules.items():
        if old not in out.columns:
            continue
        new = spec.get("rename", old)
        renames[old] = new
        dtypes[new] = spec.get("dtype")
        fills[new] = spec.get("fillna")

    if renames:
        out = out.rename(columns=renames)

    # dtypes
    for c, lbl in dtypes.items():
        if c in out.columns:
            out = _cast_dtype(out, c, lbl)

    # fillna
    for c, strat in fills.items():
        if c in out.columns:
            out = _apply_fillna(out, c, strat)

    # dedupe
    dedupe = rules.get("dedupe", {})
    subset = dedupe.get("subset")
    keep = dedupe.get("keep", "first")
    if subset and all(col in out.columns for col in subset):
        keep_arg = keep if keep in ("first","last") else False
        out = out.drop_duplicates(subset=subset, keep=keep_arg)

    # derived
    derived = rules.get("derived", {})
    for new_col, expr in derived.items():
        if not expr:
            continue
        try:
            out[new_col] = pd.eval(expr, engine="python", parser="pandas",
                                   local_dict={c: out[c] for c in out.columns})
        except Exception as e:
            print(f"[WARN] failed to derive {new_col}: {e}")

    return out
