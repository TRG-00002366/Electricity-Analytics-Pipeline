from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


from data_transformation.electric_consumer import main as run_consumer
from data_transformation.data_transform import main as run_transformation

with DAG(
    dag_id = "electricity_pipeline_consumer",
    description = "A pipeline for streaming electricity production events",
    start_date = datetime(2026,3,12),
    schedule="*/5 * * * *",
    catchup = False,
    default_args = {
        "owner" : "electricity_team",
        "retries" : 3
    }
) as dag:
    
    start = EmptyOperator(task_id = "start")
    end = EmptyOperator(task_id = "end")

    # run_consumer_task = PythonOperator(
    #     task_id = "run_consumer",
    #     python_callable = run_consumer
    # )
    run_consumer_task = BashOperator(
        task_id = "run_comsumer",
        # bash_command='''echo "This is a test of the bash"'''
        bash_command='''spark-submit \
                --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
                /opt/airflow/data_transformation/electric_consumer.py \
                --bootstrap-servers kafka:9092 \
                --duration 120'''
    )

    run_transformation_task = PythonOperator(
        task_id = "run_transformation",
        python_callable = run_transformation
    )

    start >> run_consumer_task >> run_transformation_task >> end