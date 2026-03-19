from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum
import json
import os
import shutil
from datetime import datetime

# Variable for correcting fuel type abbreviations
corrected_abbreviations = ({"battery storage" : "bats",
                            "solar battery" : "sb", 
                            "unknown energy" : "ue"})

def parse_json(line):
    return json.loads(line)

def clean_text(record, lowercase_fields):
    # Clean the string column data
    for field in lowercase_fields:
        if field in record and record[field] is not None:
            record[field] = record[field].lower().strip()

    return record

def update_fueltype(record):
    # Obtain the type name
    type_name = record.get("type-name")

    # Check if type name needs to be corrected
    if type_name in corrected_abbreviations:
        record["fueltype"] = corrected_abbreviations.get(type_name)

    return record

def build_session(appName : str ="Electricity-Analytics-Pipeline"):
    # Create a SparkSession
    spark = SparkSession.builder \
        .appName(appName) \
        .master("local[*]") \
        .getOrCreate()
    
    return spark

def save_rdd(rdd):

    # Current time for directory naming convention
    date = datetime.today().strftime("%Y-%m-%d")

    # Output directory
    output_dir = f"data/transformed/rdd_total_megawatts_by_fueltype_{date}"

    # Check if directory exists
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)

    # Save the RDD
    rdd.saveAsTextFile(output_dir)


def main():

    # Location of consumed data
    path = "/opt/airflow/data/raw/*.json"

    # Create a spark session
    spark = build_session()

    # Create a spark context
    sc = spark.sparkContext
    # json_rdd = sc.textFile("./data/temp_api_data.json")
    json_rdd = sc.textFile(path)

    # Map each json element to a single line, text elements are lowercased, fueltypes are corrected
    electricity_rdd = json_rdd.map(parse_json)\
                .map(lambda x : clean_text(x, ["respondent", "respondent-name", "fueltype", "type-name"]))\
                .map(update_fueltype) \
                .filter(lambda x : x.get("value") != 0)
    
    # Display the first 10 rows of rdd
    for row in electricity_rdd.take(10):
        print(row)

    print(f"Number of elements in rdd: {electricity_rdd.count()}")

    # Create pair RDD consisting of total megawatt production by fueltype
    pair_rdd = electricity_rdd.map(lambda x: (x["fueltype"], x["value"])) \
        .reduceByKey(lambda x, y: x + y) \
        .coalesce(1)
    
    # Display the first 10 rows of rdd
    for row in pair_rdd.take(10):
        print(row)
    
    save_rdd(pair_rdd)

    # Cleanup
    spark.stop()

if __name__ == "__main__":
    main()