from spark import spark


def main() -> None:
    # Example: Create DataFrame from a list
    data = [("Alice", 34), ("Bob", 45), ("Catherine", 29)]
    columns = ["Name", "Age"]
    
    df = spark.createDataFrame(data, schema=columns)
    df.show()
    
    spark.stop()

if __name__ == "__main__":
    main()
