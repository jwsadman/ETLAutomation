import pandas as pd
from pathlib import Path
from etl.transform import transform_with_llm
from etl.config import DATA_DIR

def main():
    csv_path = DATA_DIR / "car_prices.csv"
    df = pd.read_csv(csv_path, on_bad_lines="skip")
    cleaned = transform_with_llm(df, debug=True)
    print(cleaned.head())
    print("Rows in:", len(df), "Rows out:", len(cleaned))

if __name__ == "__main__":
    main()
