import json, re, textwrap
import pandas as pd
from .llm_agent import query_groq
from .profiling import profile_dataframe

HYBRID_RULES_PROMPT = textwrap.dedent("""
You are a senior data engineer assisting an automated ETL system.

You will be given a JSON profile of columns and a CSV sample.

Return STRICT JSON with this shape:
{
  "columns": {
    "OriginalCol": {
      "rename": "snake_case_name",
      "dtype": "int|float|string|bool|date|datetime",
      "fillna": "mean|median|mode|ffill|bfill|drop|<literal>"
    },
    ...
  },
  "dedupe": {
    "subset": ["col_a","col_b"],
    "keep": "first|last|none"
  },
  "derived": {
    "new_col": "price_usd * quantity",
    "is_luxury": "price_usd > 50000"
  }
}

Rules:
- Include only columns that exist.
- Use snake_case for rename.
- Use safe literals or strategies.
- Expressions must reference *renamed* columns.
- Output ONLY JSON. No markdown, no commentary.
""")

def _strip_code_fences(s: str) -> str:
    s = s.strip()
    if s.startswith("```"):
        s = re.sub(r"^```[a-zA-Z]*", "", s).strip()
        s = re.sub(r"```$", "", s).strip()
    return s

def get_llm_rules(df: pd.DataFrame) -> dict:
    info = profile_dataframe(df)
    prompt = f"""{HYBRID_RULES_PROMPT}

Column profile (JSON):
{json.dumps(info["profile"], indent=2)}

Sample rows (CSV):
{info["sample_csv"]}
"""
    raw = query_groq(prompt, max_tokens=2048)
    raw = _strip_code_fences(raw)
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        # minimal fallback: rename to snake_case, cast to string
        return {"columns": {c: {"rename": c.lower().replace(" ","_"), "dtype": "string", "fillna": None} for c in df.columns}}

