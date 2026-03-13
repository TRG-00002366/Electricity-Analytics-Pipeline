from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

def data_collection():
    pass

def data_transformation():
    pass

def start_producer():
    pass

with DAG(
    dag_id = "electricity_pipeline",
    description = "A pipeline for streaming electricity production events",
    start_date = datetime(2026,3,12),
    schedule="@daily",
    catchup = False,
    default_args = {
        "owner" : "electricity_team",
        "retries" : 3
    }
) as dag:
    
    start = EmptyOperator(
        task_id = "start"
    )

    data_collection_task = PythonOperator(
        task_id = "data_collection",
        python_callable = data_collection
    )

    start_producer_task = PythonOperator(
        task_id = "start_producer",
        python_callable = start_producer
    )

    data_transformation_task = PythonOperator(
        task_id = "data_transformation",
        python_callable = data_transformation
    )

    end = BashOperator(
        task_id = "end",
        bash_command = 'echo "Pipeline completed"'
    )

    start >> data_collection_task >> start_producer_task >> data_transformation_task >> end