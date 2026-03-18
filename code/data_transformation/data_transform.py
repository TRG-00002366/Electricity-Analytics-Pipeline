from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum
import json

def transform_data():

    spark = SparkSession.builder \
    .appName("Electricity-Analytics-Pipeline") \
    .master("local[*]") \
    .getOrCreate()
    json_data = None


    path = "/opt/airflow/data/raw/*/*/*.json"

    
    electricity_df = spark.read.json(path)

    electricity_df.select("*").show()

    sum_by_type = electricity_df.groupBy("type-name").agg(
        spark_sum(col("value")).alias("total_megawatthours")
    ).sort("total_megawatthours", ascending=False)
    sum_by_type.show()

    print(f"Number of records: {electricity_df.count()}")

    total_by_respondent = electricity_df.groupBy("respondent_name").agg(
        spark_sum(col("value")).alias("total_megawatthours")
    ).sort("respondent_name")
    total_by_respondent.show()

    spark.stop()

    return None

def transform_data_df():
    spark = SparkSession.builder \
    .appName("Electricity-Analytics-Pipeline") \
    .master("local[*]") \
    .getOrCreate()
    json_data = None
    # with open("./data/temp_api_data.json", 'r') as json_file:
    #     json_data = json.load(json_file)

    # print(json_data)s

    path = "/opt/airflow/data/raw/*/*/*.json"
    print("df work")


    df1 = spark.read.json(path)
    

    df1.show()



def main():

    transform_data()
    transform_data_df()

if __name__ == "__main__":
    main()