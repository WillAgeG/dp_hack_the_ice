import kagglehub
import os
import glob
import logging

from spark import spark

def load_kaggle_data() -> None:
    try:
        path = kagglehub.dataset_download("mustafakeser4/looker-ecommerce-bigquery-dataset")
    except Exception as e:
        logging.error(f"Загрузка с kaggle не удалась {e}")

    csv_files = glob.glob(os.path.join(path, '*.csv'))

    for file in csv_files:
        file_name = f"{os.path.basename(file).split('.')[0]}.parquet"
        df = spark.read.option("header", "true").csv(file)  # Read the CSV into a Spark DataFrame

        # Save the DataFrame as Parquet
        df.write.mode("overwrite").parquet(f"data/{file_name}")





