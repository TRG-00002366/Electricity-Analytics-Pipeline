'''
This module will be used to read data from the kafka topic
'''

import json
import time
from kafka import KafkaConsumer
import os

def create_consumer(topic: str = "electric_records", bootstrap_servers: str = "localhost:9092", group_id="storage_group"):
    '''
    This function will be used to create and return a kafka concsumer
    '''

    return KafkaConsumer(
        topic,
        bootstrap_servers = bootstrap_servers,
        value_deserializer = lambda v: json.loads(v.decode('utf-8')),
        key_deserializer = lambda k: k.decode("utf-8") if k else None        
    )




def read_topic(run_length: int = 20):
    consumer = create_consumer()
    start_time = time.perf_counter()
    try:
        while True:
            # Poll server for new messages
            msgs = consumer.poll()

            if time.perf_counter() - start_time > run_length:
                break
            if msgs is None:
                time.sleep(5)
            
            #Retrieves the messages from the topic sand stores them in 
            for _, messages in msgs.items():
                for message in messages:
                    record = message.value

                    if not os.path.isfile("generated_records.json"):
                        with open("generated_records.json", "w") as file:
                            file_structure = {
                                "electric_records": []
                            }
                            
                            file.write(json.dumps(file_structure))

                    with open("generated_records.json", "r+") as file:
                        #Load json file as dict
                        data = json.load(file)

                        data["electric_records"].append(record)

                        file.seek(0)

                        file.write(json.dumps(data))



    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(f"[ERROR] {e}")

read_topic()

def main():

    pass

if __name__ == "__main__":
    main()