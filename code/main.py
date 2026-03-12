from pyspark.sql import SparkSession
from data_collection.api_call import create_temp_file
from data_collection.electric_producer import send_records
from data_transformation.data_transform import transform_data
from os import getenv
from dotenv import load_dotenv, find_dotenv


def load_env():
    load_dotenv(find_dotenv())


def main():
    
    load_env()
    api_token = getenv("EIC_API_TOKEN")
#   function below is used to create a temporary json file out of the api query
    # create_temp_file(api_token)
    # transform_data()

    #Line below is called to start producer
    send_records("electric_records")

    pass

if __name__ == "__main__":
    main()