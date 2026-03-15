from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook


from kafka import KafkaProducer
from data_collection.electric_producer import main as run_producer

def connection_test():
    #conn = BaseHook.get_connection("kafka_electric_records")

    #bootstrap_servers = conn.extra_dejson.get("bootstrap.servers")

    try:
        run_producer()
    except Exception as e:
        print(f"ERROR: {e}")
      






with DAG(
    dag_id = "electricity_pipeline_producer",
    description = "A pipeline for streaming electricity production events",
    start_date = datetime(2026,3,12),
    schedule="* * * * *",
    catchup = False,
    default_args = {
        "owner" : "electricity_team",
        "retries" : 3
    }
) as dag:
    
    start = EmptyOperator(task_id = "start")
    end = EmptyOperator(task_id = "end")

    run_producer_task = PythonOperator(
        task_id = "start_producer",
        python_callable = connection_test
        #python_callable = run_producer
    )

    start >> run_producer_task >> end