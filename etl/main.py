import logging

from extract.load_data import load_kaggle_data
from trasform.transform_data import get_products
from spark import spark


def main() -> None:
    logging.info("Старт ETL процесса")
    load_kaggle_data()
    get_products()


    spark.stop()
    logging.info("Конец ETL процесса")

if __name__ == "__main__":
    main()