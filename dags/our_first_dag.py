# dags/daily_etl_pipeline_airflow3.py
from airflow.sdk import dag, task
from airflow import Dataset
from datetime import datetime, timedelta
import os
import pandas as pd
import random
import mysql.connector


# 2️⃣ Default arguments used by scheduler and executor
default_args = {
    "owner": "Brayan Opiyo",
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}


# 3️⃣ DAG definition — runs daily *or* when raw_events dataset updates
@dag(
    dag_id="daily_etl_pipeline_airflow3",
    description="ETL workflow using Airflow 3 and MySQL integration (TaskFlow API, datasets, retry logic)",
    schedule="@daily",
    start_date=datetime(2025, 10, 7),
    catchup=False,
    default_args=default_args,
    tags=["airflow3", "etl", "mysql"],
)
def daily_etl_pipeline():
    # Set up local paths
    output_dir = "/opt/airflow/tmp"
    raw_path = os.path.join(output_dir, "raw_events.csv")
    transformed_path = os.path.join(output_dir, "transformed_events.csv")

    # --- E (Extract) ---
    @task
    def generate_fake_events():
        """Simulates pulling new event data and saving to CSV."""
        events = [
            "Solar flare near Mars(opiman)", "New AI model released", "Fusion milestone",
            "Celestial event tonight", "Economic policy update", "Storm in Nairobi",
            "New particle at CERN", "NASA Moon base plan", "Tremors in Tokyo", "Open-source boom",
        ]
        sample_events = random.sample(events, 5)
        data = {
            "timestamp": [datetime.now().strftime("%Y-%m-%d %H:%M:%S") for _ in sample_events],
            "event": sample_events,
            "intensity_score": [round(random.uniform(1, 10), 2) for _ in sample_events],
            "category": [random.choice(["Science", "Tech", "Weather", "Space", "Finance"]) for _ in sample_events],
        }
        df = pd.DataFrame(data)
        os.makedirs(output_dir, exist_ok=True)
        df.to_csv(raw_path, index=False)
        print(f"[RAW] Saved to {raw_path}")
        return raw_path

    # --- T (Transform) ---
    @task
    def transform_and_save_csv(raw_file: str):
        """Cleans and sorts event data."""
        df = pd.read_csv(raw_file)
        df_sorted = df.sort_values(by="intensity_score", ascending=False)
        df_sorted.to_csv(transformed_path, index=False)
        print(f"[TRANSFORMED] Sorted and saved to {transformed_path}")
        return transformed_path

    # --- L (Load to MySQL) ---
    @task
    def load_to_mysql(transformed_file: str):
        """Loads the transformed CSV data into a MySQL table."""
        import pandas as pd
        import mysql.connector

        # Database credentials (should match your Airflow MySQL connection)
        db_config = {
            "host": "host.docker.internal",        # or "localhost" if running locally
            "user": "airflow",
            "password": "airflow",
            "database": "airflow_db",
            "port": 3306
        }

        table_name = "transformed_events"
        df = pd.read_csv(transformed_file)

        # Connect to MySQL
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        # Create table if it doesn’t exist
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                timestamp VARCHAR(50),
                event VARCHAR(255),
                intensity_score FLOAT,
                category VARCHAR(100)
            );
        """)

        # Replace existing data
        cursor.execute(f"TRUNCATE TABLE {table_name};")

        # Insert new data
        for _, row in df.iterrows():
            cursor.execute(
                f"INSERT INTO {table_name} (timestamp, event, intensity_score, category) VALUES (%s, %s, %s, %s)",
                tuple(row)
            )

        conn.commit()
        conn.close()
        print(f"[LOAD] Data successfully loaded into MySQL table: {table_name}")

    # --- Preview MySQL Table ---
    from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


    # preview_mysql = MySqlOperator(
    #     task_id="preview_mysql_table",
    #     mysql_conn_id="local_mysql",
    #     sql="SELECT * FROM transformed_events LIMIT 5;",
    #     do_xcom_push=True,
    # )
    preview_mysql = SQLExecuteQueryOperator(
    task_id="preview_mysql_table",
    conn_id="local_mysql",  # ✅ use your actual connection id
    sql="SELECT * FROM transformed_events LIMIT 5;",
    do_xcom_push=True,
)

   

    # --- Define task dependencies (implicit with TaskFlow API) ---
    raw = generate_fake_events()
    transformed = transform_and_save_csv(raw)
    load_to_mysql(transformed) >> preview_mysql


# 4️⃣ Instantiate the DAG
dag = daily_etl_pipeline()
