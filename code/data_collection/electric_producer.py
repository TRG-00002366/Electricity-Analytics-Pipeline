'''
Electric Record Producer
'''

from kafka import KafkaProducer
import json
import time
from datetime import datetime




def create_producer(bootstrap_servers: str = "localhost:9092"):
    
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        compression_type="lz4",
        key_serializer=lambda k: k.encode('utf-8') if k else None,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )


def generate_records():
    
    

    record = {
    "period" : "2026-03-02T07",
    "respondent" : "AVA",
    "respondent_name" : "Avista Corporation",
    "fueltype" : "WAT",
    "type-name" : "Hydro", 
    "value" : "729",
    "value-units" : "megawatthours"
    }

    return record

def send_records(topic: str = "electric_records", count: int = 100, run_length: int = 20):

    producer = create_producer()
    start_time = time.perf_counter()
    try:
        count = 0
        start = time.time()

        while True:

            if time.perf_counter() - start_time > run_length:
                break


            #Function call generates record
            record = generate_records()
            producer.send(
                topic=topic,
                key=record["respondent"],
                value=record
            )


            time.sleep(5)

            count += 1



    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()



def main():
    producer = create_producer()





if __name__ == "__main__":
    main()