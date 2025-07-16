
from langgraph.graph import StateGraph, END

def build_etl_graph():
    sg = StateGraph(dict)

    # Node functions
    from etl.extract import extract_from_csv
    from etl.transform import transform_with_llm
    from etl.load import load_to_postgres

    sg.add_node("extract", lambda state: {"df": extract_from_csv(state["file_path"])})
    sg.add_node("transform", lambda state: {"df": transform_with_llm(state["df"])})
    sg.add_node("load", lambda state: {"status": load_to_postgres(state["df"], state["table"], state["conn"])})
    
    sg.set_entry_point("extract")
    sg.add_edge("extract", "transform")
    sg.add_edge("transform", "load")
    sg.add_edge("load", END)

    return sg.compile()
