from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum
import json

def build_session(appName : str ="Electricity-Analytics-Pipeline"):
    # Create a SparkSession
    spark = SparkSession.builder \
        .appName(appName) \
        .master("local[*]") \
        .getOrCreate()
    
    return spark


def main():

    path = "/opt/airflow/data/raw/*.json"

    spark = build_session()

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


if __name__ == "__main__":
    main()