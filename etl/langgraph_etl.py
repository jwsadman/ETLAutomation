from typing import TypedDict
import pandas as pd
from langgraph.graph import StateGraph, END
from .extract import extract_from_csv
from .transform import transform_with_llm
from .load import load_to_postgres

class ETLState(TypedDict, total=False):
    file_path: str
    table: str
    conn: str
    df: pd.DataFrame
    status: str
    rows: int

def build_etl_graph():
    def node_extract(state: ETLState):
        df = extract_from_csv(state["file_path"])
        return {"df": df}

    def node_transform(state: ETLState):
        cleaned_df = transform_with_llm(state["df"])
        return {"df": cleaned_df}

    def node_load(state: ETLState):
        df = state["df"]
        load_to_postgres(df, state["table"], state["conn"])
        return {"status": "loaded", "rows": len(df)}

    sg = StateGraph(ETLState)
    sg.add_node("extract", node_extract)
    sg.add_node("transform", node_transform)
    sg.add_node("load", node_load)

    sg.set_entry_point("extract")
    sg.add_edge("extract", "transform")
    sg.add_edge("transform", "load")
    sg.add_edge("load", END)

    return sg.compile()


