
from prefect import flow
from etl.langgraph_etl import build_etl_graph

@flow(log_prints=True)
def run_etl_pipeline(file_path: str, table: str, conn: str):
    etl_graph = build_etl_graph()
    etl_graph.invoke({"file_path": file_path, "table": table, "conn": conn})

if __name__ == "__main__":
    run_etl_pipeline("car_prices.csv", "cleaned_data", "postgresql://postgres:admin123@localhost:5432/mydb")
