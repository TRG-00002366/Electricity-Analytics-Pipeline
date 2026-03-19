from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum as spark_sum
import json

def build_session(appName : str ="Electricity-Analytics-Pipeline"):
    # Create a SparkSession
    spark = SparkSession.builder \
        .appName(appName) \
        .master("local[*]") \
        .getOrCreate()
    
    return spark

def save_df(df, path_name):

    df.write \
        .option("header", "true") \
        .mode("overwrite") \
        .csv(f"data/transformed/{path_name}")

def main():

    path = "/opt/airflow/data/raw/*.json"

    # Create a spark session
    spark = build_session()

    electricity_df = spark.read.json(path)

    electricity_df.select("*").show()

    sum_by_type = electricity_df.groupBy("type-name").agg(
        spark_sum(col("value")).alias("total_megawatthours")
    ).sort("total_megawatthours", ascending=False)
    sum_by_type.show()

    save_df(sum_by_type, "production_by_fuel_type")

    print(f"Number of records: {electricity_df.count()}")

    total_by_respondent = electricity_df.groupBy("respondent_name").agg(
        spark_sum(col("value")).alias("total_megawatthours")
    ).sort("respondent_name")
    total_by_respondent.show()

    save_df(total_by_respondent, "production_by_respondent_name")

    avg_hourly_by_type = electricity_df.groupBy("fueltype", "type-name").agg(
        avg(col("value")).alias("average_hourly_production")
    ).sort("average_hourly_production", ascending=False)
    avg_hourly_by_type.show()

    save_df(avg_hourly_by_type, "average_hourly_production_by_fuel_type")

    # Clean up
    spark.stop()


if __name__ == "__main__":
    main()