# ETL Automation Pipeline

## 📌 Overview
This project is an end-to-end ETL (Extract, Transform, Load) pipeline built using:

Prefect → For workflow orchestration and task management

LangGraph → For graph-based ETL orchestration and automation

Pandas → For data transformation and cleaning

PostgreSQL → For storing the cleaned data

SQLAlchemy → For database interaction

Python → Core language for ETL logic

The pipeline automatically extracts data from CSV, applies transformation rules, and loads it into a PostgreSQL database. It also includes automatic database creation if the target database does not exist.


## ⚙️ Prerequisites
Python 3.11+

PostgreSQL installed and running on localhost:5432

pip or conda for Python dependencies


### 🚀 How to Run?

1. Create an environment

```bash
conda create -n etl python=3.11


conda activate etl

```

2. install requirements

```bash
pip install -r requirements.txt
```

3. Create a .env file which has the following: 

GROQ_API_KEY=YOUR_REAL_KEY_HERE

GROQ_MODEL=llama-3.1-8b-instant

DB_URL=your actual PostgreSQL DB credentials

FILE_PATH=your CSV file path

TABLE=DB table name

4. Run the ETL Pipeline

```bash
python -m etl.flow
```

## 📜 License
MIT License © 2025 [Md Sadman]
