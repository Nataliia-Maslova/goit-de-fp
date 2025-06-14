from prefect import task, flow
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import re

def clean_text(text):
    return re.sub(r'[^a-zA-Z0-9,.\\"\']', '', str(text))

clean_text_udf = udf(clean_text, StringType())

@task
def clean_and_deduplicate(table_name: str):
    spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()
    df = spark.read.parquet(f"bronze/{table_name}")

    for col_name, dtype in df.dtypes:
        if dtype == 'string':
            df = df.withColumn(col_name, clean_text_udf(df[col_name]))

    df = df.dropDuplicates()
    output_path = f"silver/{table_name}"
    df.write.mode("overwrite").parquet(output_path)
    print(f"Cleaned and saved to {output_path}")
    spark.stop()

@flow
def bronze_to_silver():
    for table in ["athlete_bio", "athlete_event_results"]:
        clean_and_deduplicate(table)

if __name__ == "__main__":
    bronze_to_silver()