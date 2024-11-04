import logging
from pyspark.sql import SparkSession


def create_spark_session() -> SparkSession:
    spark = SparkSession.builder \
        .master('local') \
        .appName('myAppName') \
        .config('spark.executor.memory', '2gb') \
        .config("spark.cores.max", "2") \
        .getOrCreate()
    
    logging.info('Spark Created')
    return spark


spark = create_spark_session()