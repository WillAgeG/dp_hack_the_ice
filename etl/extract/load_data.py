import kagglehub
import os
import glob
import logging

from spark import spark

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

schemas = {
    "products": StructType([
        StructField("id", StringType(), True),
        StructField("cost", DoubleType(), True),
        StructField("category", StringType(), True),
        StructField("name", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("retail_price", DoubleType(), True),
        StructField("department", StringType(), True),
        StructField("sku", StringType(), True),
        StructField("distribution_center_id", IntegerType(), True)
    ]),

    "orders": StructType([
        StructField("order_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("status", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("returned_at", TimestampType(), True),
        StructField("shipped_at", TimestampType(), True),
        StructField("delivered_at", TimestampType(), True),
        StructField("num_of_item", IntegerType(), True)
    ]),

    "inventory_items": StructType([
        StructField("id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("sold_at", TimestampType(), True),
        StructField("cost", DoubleType(), True),
        StructField("product_category", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("product_brand", StringType(), True),
        StructField("product_retail_price", DoubleType(), True),
        StructField("product_department", StringType(), True),
        StructField("product_sku", StringType(), True),
        StructField("product_distribution_center_id", IntegerType(), True)
    ]),

    "users": StructType([
        StructField("id", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("gender", StringType(), True),
        StructField("state", StringType(), True),
        StructField("street_address", StringType(), True),
        StructField("postal_code", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("traffic_source", StringType(), True),
        StructField("created_at", TimestampType(), True)
    ]),

    "distribution_centers": StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True)
    ]),

    "events": StructType([
        StructField("id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("sequence_number", IntegerType(), True),
        StructField("session_id", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("ip_address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("postal_code", StringType(), True),
        StructField("browser", StringType(), True),
        StructField("traffic_source", StringType(), True),
        StructField("uri", StringType(), True),
        StructField("event_type", StringType(), True)
    ]),

    "order_items": StructType([
        StructField("id", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("inventory_item_id", StringType(), True),
        StructField("status", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("shipped_at", TimestampType(), True),
        StructField("delivered_at", TimestampType(), True),
        StructField("returned_at", TimestampType(), True),
        StructField("sale_price", DoubleType(), True)
    ])
}

def load_kaggle_data() -> None:
    try:
        path = kagglehub.dataset_download("mustafakeser4/looker-ecommerce-bigquery-dataset")
    except Exception as e:
        logging.error(f"Загрузка с kaggle не удалась {e}")

    csv_files = glob.glob(os.path.join(path, '*.csv'))

    for file in csv_files:
        file_name = os.path.basename(file).split('.')[0]
        df = spark.read.option("header", "true").csv(file, schema=schemas[file_name])
    
        file_parquet = f"{file_name}.parquet"
        df.write.mode("overwrite").parquet(f"data/{file_parquet}")





