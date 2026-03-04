from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum
import json


def transform_data():

    spark = SparkSession.builder \
    .appName("Electricity-Analytics-Pipeline") \
    .master("local[*]") \
    .getOrCreate()
    json_data = None
    # with open("./data/temp_api_data.json", 'r') as json_file:
    #     json_data = json.load(json_file)

    # print(json_data)


    electricity_df = spark.read.json("./data/temp_api_data.json")

    electricity_df.select("*").show()

    sum_by_type = electricity_df.groupBy("type-name").agg(
        spark_sum(col("value")).alias("total_megawatthours")
    ).sort("total_megawatthours", ascending=False)
    sum_by_type.show()



    print(f"Number of records: {electricity_df.count()}")




    spark.stop()

    return None