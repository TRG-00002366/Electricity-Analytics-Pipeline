'''
This module will be used to read data from the kafka topic
'''

import json
import time
from kafka import KafkaConsumer
import os
from datetime import date
from pathlib import Path

BASE_PATH = Path("/opt/airflow/data")

def create_consumer(topic: str = "electric_records", bootstrap_servers: str = "kafka:9092", group_id="storage_group"):
    '''
    This function will be used to create and return a kafka concsumer
    '''

    return KafkaConsumer(
        topic,
        bootstrap_servers = bootstrap_servers,
        group_id = group_id,
        value_deserializer = lambda v: json.loads(v.decode('utf-8')),
        key_deserializer = lambda k: k.decode("utf-8") if k else None        
    )

def get_output_path_name(period: str):
    yyyy_mm, hh = period.split("T")
    path_name = BASE_PATH / f"raw/{yyyy_mm}/{hh}/records.json"
    path_name.parent.mkdir(parents=True, exist_ok=True)
    return path_name

def save_records(records):

    records_by_period = {}

    for record in records:
        period = record.get("period")
        if not period:
            continue
        if period not in records_by_period:
            records_by_period[period] = []

        records_by_period[period].append(record)

    # Write grouped records
    for period, period_records in records_by_period.items():
        output_path = get_output_path_name(period)

        with open(output_path, "a") as file:
            for record in period_records:
                file.write(json.dumps(record) + "\n")

def read_topic(run_length: int = 20):
    consumer = create_consumer()
    start_time = time.perf_counter()
    records = []
    count = 0

    try:
        while True:
            # Poll server for new messages
            msgs = consumer.poll(timeout_ms=1000)

            if time.perf_counter() - start_time > run_length:
                break
            if not msgs:
                time.sleep(5)
                continue

            #Retrieves the messages from the topic and stores them
            for _, messages in msgs.items():
                for message in messages:
                    records.append(message.value)
                    count += 1

            if records:
                save_records(records)
                records = []

        print(f"[INFO] Consumer consumed {count} records")
    
    except KeyboardInterrupt:
        pass

    except Exception as e:
        print(f"[ERROR] {e}")

    finally:
        consumer.close()

def main():

    read_topic()

if __name__ == "__main__":
    main()