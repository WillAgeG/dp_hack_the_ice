from spark import spark


def get_products():
    df = spark.read.parquet("data/products.parquet")
    df.show()
    