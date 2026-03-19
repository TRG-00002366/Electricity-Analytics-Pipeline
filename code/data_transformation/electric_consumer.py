'''
This module will be used to read data from the kafka topic
'''

import json
import time
from kafka import KafkaConsumer
import os
from datetime import date
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType
from pyspark.sql.functions import col, from_json

BASE_PATH = Path("/opt/airflow/data")

#This is used to define the structure of the json being streamed
json_schema = StructType() \
    .add("period", StringType()) \
    .add("respondent", StringType()) \
    .add("respondent_name", StringType()) \
    .add("fueltype", StringType()) \
    .add("type-name", StringType()) \
    .add("value", IntegerType()) \
    .add("value-units", StringType())

def create_consumer(topic: str = "electric_records", bootstrap_servers: str = "kafka:9092", group_id="storage_group"):
    '''
    This function will be used to create and return a kafka consumer
    '''

    return KafkaConsumer(
        topic,
        bootstrap_servers = bootstrap_servers,
        group_id = group_id,
        value_deserializer = lambda v: json.loads(v.decode('utf-8')),
        key_deserializer = lambda k: k.decode("utf-8") if k else None        
    )

def read_topic(run_length: int = 20):

    spark = SparkSession.builder.appName("Stream Consumer").getOrCreate()
    #consumer = create_consumer()
    # start_time = time.perf_counter()
    records = []
    count = 0
    try:
        df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("subscribe", "electric_records") \
            .load()
        
        #This line is used to clean 
        cleaned_df = df.select(from_json(col("value").cast("string"), json_schema).alias("data")).select("data.*")
        

        query = cleaned_df.writeStream \
            .outputMode("append") \
            .format("json") \
            .option("path", f"/opt/airflow/data/raw/") \
            .option("checkpointLocation", "/opt/airflow/data/checkpoints/electric_records") \
            .start()
                
        query.awaitTermination(timeout=120)
        query.stop()
    except Exception as e:
        print(f"[ERROR]     {e}")
    

    spark.stop()

def main():

    read_topic()

if __name__ == "__main__":
    main()