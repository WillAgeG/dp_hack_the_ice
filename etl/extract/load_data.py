import kagglehub


def load_data() -> None:
    path = kagglehub.dataset_download("mustafakeser4/looker-ecommerce-bigquery-dataset")

    print("Path to dataset files:", path)