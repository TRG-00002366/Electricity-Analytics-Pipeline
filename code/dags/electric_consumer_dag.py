from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# Default arguments for DAG setup
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

    run_rdd_etl_task = BashOperator(
        task_id = "run_rdd_etl",
        bash_command = """spark-submit \
            --master spark://spark-master:7077 \
            --deploy-mode client \
            /opt/airflow/data_transformation/batch_rdd_etl.py"""
    )
 
    run_df_etl_task = BashOperator(
        task_id = "run_df_etl",
        bash_command = """spark-submit \
            --master spark://spark-master:7077 \
            --deploy-mode client \
            /opt/airflow/data_transformation/batch_df_etl.py"""
    )

    start >> run_consumer_task >> [run_rdd_etl_task, run_df_etl_task] >> end