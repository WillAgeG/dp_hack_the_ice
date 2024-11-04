from pyspark.sql import SparkSession


def create_spark_session():
    # Explicitly set JAVA_HOME (optional if Dockerfile changes work)
    # os.environ["JAVA_HOME"] = "/usr/lib/jvm/default-java"

    spark = SparkSession.builder \
        .master('local') \
        .appName('myAppName') \
        .config('spark.executor.memory', '2gb') \
        .config("spark.cores.max", "2") \
        .getOrCreate()
    
    print('Spark Created')
    return spark


spark = create_spark_session()