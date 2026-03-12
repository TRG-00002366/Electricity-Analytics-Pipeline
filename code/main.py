from pyspark.sql import SparkSession
from data_collection.api_call import create_temp_file
from data_collection.electric_producer import send_records
from data_transformation.data_transform import transform_data
from data_transformation.electric_consumer import read_topic
from os import getenv
from dotenv import load_dotenv, find_dotenv
import threading
import signal
import time

def load_env():
    load_dotenv(find_dotenv())


def main():
    
    load_env()
    api_token = getenv("EIC_API_TOKEN")
#   function below is used to create a temporary json file out of the api query
    # create_temp_file(api_token)
    # transform_data()
    try:
        run_length = int(input("How long do you want the program to run?"))


        #Line below is called to start the consumer
        consumer_thread = threading.Thread(target=read_topic(run_length=run_length))
        consumer_thread.start()

        #Line below is called to start producer
        producer_thread = threading.Thread(target=send_records(run_length=run_length))
        producer_thread.start()

        consumer_thread.join()
        producer_thread.join()
        print("Both threads closed successfully")




    except ValueError:
        print("Input a valid number of seconds!")





    # read_topic()



    # send_records("electric_records")




    pass

if __name__ == "__main__":
    main()