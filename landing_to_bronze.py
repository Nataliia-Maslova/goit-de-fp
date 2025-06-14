from prefect import task, flow
from pyspark.sql import SparkSession
import requests
import os

@task
def download_data(table_name: str):
    url = f"https://ftp.goit.study/neoversity/{table_name}.csv"
    local_path = f"{table_name}.csv"
    print(f"Downloading from {url}")
    response = requests.get(url)
    if response.status_code == 200:
        with open(local_path, 'wb') as f:
            f.write(response.content)
        print(f"Downloaded and saved as {local_path}")
    else:
        raise Exception(f"Failed to download: {response.status_code}")
    return local_path

@task
def convert_csv_to_parquet(csv_path: str, table_name: str):
    spark = SparkSession.builder.appName("LandingToBronze").getOrCreate()
    df = spark.read.option("header", True).csv(csv_path)
    output_path = f"bronze/{table_name}"
    df.write.mode("overwrite").parquet(output_path)
    print(f"Saved to {output_path}")
    spark.stop()

@flow
def landing_to_bronze(table_name: str):
    csv_path = download_data(table_name)
    convert_csv_to_parquet(csv_path, table_name)

if __name__ == "__main__":
    for table in ["athlete_bio", "athlete_event_results"]:
        landing_to_bronze(table)