from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum
import json

# Variable for correcting fuel type abbreviations
corrected_abbreviations = ({"battery storage" : "bats",
                            "solar battery" : "sb", 
                            "unknown energy" : "ue"})

def parse_json(line):
    return json.loads(line)

def clean_text(record, lowercase_fields):

    for field in lowercase_fields:
        if field in record and record[field] is not None:
            record[field] = record[field].lower().strip()

    return record

def update_fueltype(record):

    type_name = record.get("type-name")

    if type_name in corrected_abbreviations:
        record["fueltype"] = corrected_abbreviations.get(type_name)

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

    path = "/opt/airflow/data/raw/*/*/*.json"

    # Create a spark context
    sc = spark.sparkContext
    # json_rdd = sc.textFile("./data/temp_api_data.json")
    json_rdd = sc.textFile(path)


    # Map each json element to a single line, text elements are lowercased, fueltypes are corrected
    electricity_rdd = json_rdd.map(parse_json)\
                .map(lambda x : clean_text(x, ["respondent", "respondent-name", "fueltype", "type-name"]))\
                .map(update_fueltype)
    
    # Display the first 5 rows of rdd
    print(electricity_rdd.take(5))

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
    #path = "/opt/airflow/data/raw/*/*.json"

    df1 = spark.read.json(path)
    

    df1.show()



def main():

    transform_data()
    transform_data_df()

if __name__ == "__main__":
    main()