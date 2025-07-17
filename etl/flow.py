from prefect import flow
from .config import FILE_PATH, DB_URL, TABLE
from .langgraph_etl import build_etl_graph

@flow(name="run-etl-pipeline", log_prints=True)
def run_etl_pipeline(
    file_path: str = FILE_PATH,
    table: str = TABLE,
    conn: str = DB_URL,
):
    print(f"[Flow] file={file_path} table={table} conn={conn}")
    etl_graph = build_etl_graph()
    result = etl_graph.invoke({"file_path": file_path, "table": table, "conn": conn})
    print("[Flow] result:", result)

if __name__ == "__main__":
    run_etl_pipeline()


