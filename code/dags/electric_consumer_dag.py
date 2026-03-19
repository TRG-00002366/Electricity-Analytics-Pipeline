from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from data_transformation.electric_consumer import main as run_consumer
from data_transformation.batch_df_etl import main as run_df_etl
from data_transformation.batch_rdd_etl import main as run_rdd_etl

default_args = {
        "owner" : "electricity_team",
        "start_date" : datetime(2026,3,12),
        "retries" : 3,
        "retry_delay": timedelta(minutes=5),
    }

with DAG(
    dag_id = "electricity_pipeline_consumer",
    description = "A pipeline for streaming electricity production events",
    start_date = datetime(2026,3,12),
    schedule="*/5 * * * *",
    catchup = False,
    default_args = default_args
) as dag:
    
    start = EmptyOperator(task_id = "start")
    end = EmptyOperator(task_id = "end")

    run_consumer_task = BashOperator(
        task_id = "run_consumer",
        # bash_command='''echo "This is a test of the bash"'''
        bash_command='''spark-submit \
                --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
                /opt/airflow/data_transformation/electric_consumer.py \
                --bootstrap-servers kafka:9092 \
                --duration 120'''
    )

    run_rdd_etl_task = PythonOperator(
        task_id = "run_rdd_etl",
        python_callable = run_rdd_etl
    )

    run_df_etl_task = PythonOperator(
        task_id = "run_df_etl",
        python_callable = run_df_etl
    )

    start >> run_consumer_task >> run_rdd_etl_task >> run_df_etl_task >> end