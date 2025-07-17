import pandas as pd
from .rules import get_llm_rules
from .apply_rules import apply_llm_rules

def transform_with_llm(df: pd.DataFrame, debug=False) -> pd.DataFrame:
    rules = get_llm_rules(df)
    if debug:
        import pprint; pprint.pprint(rules)
    cleaned = apply_llm_rules(df, rules)
    return cleaned


