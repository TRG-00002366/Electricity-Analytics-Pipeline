from pyspark.sql import SparkSession
from data_collection.api_call import create_temp_file
from data_transformation.data_transform import transform_data

def main():
    print("Foo")
#   function below is used to create a temporary json file out of the api query
    #create_temp_file()
    transform_data()
    
    pass

if __name__ == "__main__":
    main()