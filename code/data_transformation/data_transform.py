from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
import json

def parse_json(line):

    data = json.loads(line)

    for record in data:
        yield record

def clean_text(record, lowercase_fields):

    for field in lowercase_fields:
        if field in record and record[field] is not None:
            record[field] = record[field].lower().strip()

    return record

def deduplicate_fueltype(record):

    if "fueltype" in record and record["fueltype"] is not None:
        if "type-name" == "battery storage":
            record["fueltype"] = "BATS"
        if "type-name" == "solar battery":
            record["fueltype"] = "SB"
        if "type-name" == "unknown energy'":
            record["fueltype"] = "UE"

    return record

def transform_data():

    spark = SparkSession.builder \
    .appName("Electricity-Analytics-Pipeline") \
    .master("local[*]") \
    .getOrCreate()
    json_data = None
    # with open("./data/temp_api_data.json", 'r') as json_file:
    #     json_data = json.load(json_file)

    # print(json_data)

    # Create a spark context
    sc = spark.sparkContext
    json_rdd = sc.textFile("./data/temp_api_data.json")

    # Map each json element to a single line, text elements are lowercased, duplicate fuel types are addressed
    electricity_rdd = json_rdd.flatMap(parse_json)\
                .map(lambda x : clean_text(x, ["respondent", "respondent-name", "fueltype", "type-name"]))\
                .map(deduplicate_fueltype)
    # Display the first 5 rows of rdd
    print(electricity_rdd.take(5))

    electricity_df = spark.read.json("./data/temp_api_data.json")

    electricity_df.select("*").show()

    print(f"Number of records: {electricity_df.count()}")


    spark.stop()

    return None