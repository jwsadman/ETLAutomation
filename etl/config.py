import os
from pathlib import Path
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")

GROQ_API_KEY = os.getenv("GROQ_API_KEY")
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.1-8b-instant")

DB_URL = os.getenv("DB_URL")
FILE_PATH = os.getenv("FILE_PATH", str(PROJECT_ROOT / "data" / "car_prices.csv"))
TABLE = os.getenv("TABLE", "cleaned_data")

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
DATA_DIR = PROJECT_ROOT / "data"
