from pyspark.sql import SparkSession
import os

from extract import load_data

def create_spark_session():
    # Explicitly set JAVA_HOME (optional if Dockerfile changes work)
    # os.environ["JAVA_HOME"] = "/usr/lib/jvm/default-java"

    spark = SparkSession.builder \
        .appName("prediction") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

    print('Spark Created')
    return spark

def main():
    spark = create_spark_session()
    # Example: Create DataFrame from a list
    data = [("asdasd", 34), ("dsadsa", 45), ("sssss", 29)]
    columns = ["Name", "Age"]
    
    df = spark.createDataFrame(data, schema=columns)
    df.show()
    load_data()
    spark.stop()

if __name__ == "__main__":
    main()