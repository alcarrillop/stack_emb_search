from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from main import fetch_questions_and_answers, check_existing_records

# Define los argumentos por defecto del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 28),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define tu DAG
with DAG(
    'stack_overflow_data_ingestion',
    default_args=default_args,
    description='Ingest Stack Overflow data into PostgreSQL',
    schedule='@daily',
) as dag:
    # Define las tareas de tu DAG
    fetch_data_task = PythonOperator(
        task_id='fetch_data_task',
        python_callable=fetch_questions_and_answers,
    )

    check_existing_records_task = PythonOperator(
        task_id='check_existing_records_task',
        python_callable=check_existing_records
    )

    # Establece las dependencias entre las tareas
    fetch_data_task >> check_existing_records_task
