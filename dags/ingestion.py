import os
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# ----------------- Functions ----------------- 
def read_files(source_dir_path, prefix): 
    # Get a list of all files in the current directory
    files = os.listdir(source_dir_path)

    # Filter the list to only include extension files
    list_files = [f for f in files if f.startswith(prefix)]
    list_files.sort()
    # Return the list of files
    return list_files

def csv_to_df(file_path, chunksize): 
    chunks = pd.read_csv(file_path, chunksize=chunksize)
    df     = pd.concat(chunks)
    # drop first column of DataFrame
    del df[df.columns[0]]
    return df

def load_data_to_postgres(df, table, postgres_conn_id):
    postgres_hook = PostgresHook(postgres_conn_id)
    engine = postgres_hook.get_sqlalchemy_engine()
    df.to_sql(table, engine, if_exists="replace", chunksize=1000, index=False)

def ingestion(**kwargs): 
    table = kwargs["table"]
    source_dir_path = kwargs["source_dir_path"]
    postgres_conn_id = kwargs["postgres_conn_id"]

    # Initialize an empty list to store the DataFrame chunks
    data_chunks = []

    files = read_files(source_dir_path, table)
    print(files)
    for file in files:
        df = csv_to_df(file_path=source_dir_path+file, chunksize=500)
        data_chunks.append(df)

    # Concatenate the chunks into a single DataFrame
    df = pd.concat(data_chunks)

    # Load to postgres
    load_data_to_postgres(df, table, postgres_conn_id)


#  ----------------- DAG Configuration ----------------- 
doc_md_DAG = """
### Ingestion DAG
The purpose is ingest all data sources to datawarehouse
"""

default_args = {
    'owner'      : 'ardhi',
    'retry_delay': timedelta(minutes=5),
    'start_date' : datetime(2023, 11, 29)
}

with DAG(
    dag_id            = 'ingestion',
    schedule_interval = '0 0 * * *',  # UTC
    default_args      = default_args,
    catchup           = False,
    doc_md            = doc_md_DAG):

    # Define each tasks
    start = EmptyOperator(task_id="start")

    # Source path variables
    source_dir_path = "data/"
    postgres_conn_id = "pg_dwh"
    tables = ["customer"]

    for table in tables:
        ingest_task = PythonOperator(
            task_id         = f"ingest_{table}",
            python_callable = ingestion,
            op_kwargs       = {"source_dir_path": source_dir_path, "table": table, "postgres_conn_id": postgres_conn_id}
        )

    end = EmptyOperator(task_id="end")

    # Define flow
    start >> ingest_task >> end

   