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

    spark = build_session()

    

if __name__ == "__main__":
    main()