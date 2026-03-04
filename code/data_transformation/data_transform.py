from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
import json


def transform_data():

    spark = SparkSession.builder \
    .appName("Electricity-Analytics-Pipeline") \
    .master("local[*]") \
    .getOrCreate()
    json_data = None
    with open("./data/temp_api_data.json", 'r') as json_file:
        json_data = json.load(json_file)

    # print(json_data)


    electricity_df = spark.read.json("./data/temp_api_data.json")

    electricity_df.select("period", "respondent", "fueltype", "value", "value-units").show()


    spark.stop()

    return None