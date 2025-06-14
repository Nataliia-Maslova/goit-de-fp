from prefect import task, flow
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, current_timestamp

@task
def aggregate_avg_stats():
    spark = SparkSession.builder.appName("SilverToGold").getOrCreate()

    # Read parquet files from silver layer
    bio = spark.read.parquet("silver/athlete_bio")
    results = spark.read.parquet("silver/athlete_event_results")

    # Convert weight and height to float
    bio = (
        bio.withColumn("weight", col("weight").cast("float"))
            .withColumn("height", col("height").cast("float"))
    )

    # Join datasets
    joined = results.join(bio, on="athlete_id", how="inner")

    # Aggregate
    agg = (
        joined.groupBy("sport", "medal", "sex", "country_noc")
        .agg(
            avg("weight").alias("avg_weight"),
            avg("height").alias("avg_height")
        )
        .withColumn("timestamp", current_timestamp())
    )

    # Save to gold layer
    agg.write.mode("overwrite").parquet("gold/avg_stats")
    print("Saved to gold/avg_stats")

    spark.stop()

@flow
def silver_to_gold():
    aggregate_avg_stats()

if __name__ == "__main__":
    silver_to_gold()
    