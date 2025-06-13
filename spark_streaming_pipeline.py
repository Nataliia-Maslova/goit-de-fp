from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, avg
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# --- Налаштування ---
kafka_bootstrap_servers = "localhost:9092"  
input_topic = "athlete_event_results"
output_topic = "enriched_athlete_data"
output_table = "avg_athlete_metrics"

# --- MySQL конфіг ---
jdbc_url = "jdbc:mysql://217.61.57.46:3306/olympic_dataset"
jdbc_user = "neo_data_admin"
jdbc_password = "Proyahaxuqithab9oplp"

# --- Spark сесія ---
spark = SparkSession.builder \
    .appName("SparkKafkaMySQLPipeline") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# --- 1. Зчитування біо-даних ---
bio_df = spark.read.format('jdbc').options(
    url=jdbc_url,
    driver='com.mysql.cj.jdbc.Driver',
    dbtable="athlete_bio",
    user=jdbc_user,
    password=jdbc_password
).load()

# --- 2. Фільтрація порожніх/нечислових значень ---
bio_df = bio_df \
    .filter((col("height").cast("double").isNotNull()) & (col("weight").cast("double").isNotNull()))

# --- 3. Читання з Kafka ---
schema = StructType([
    StructField("athlete_id", IntegerType()),
    StructField("sport", StringType()),
    StructField("event", StringType()),
    StructField("country_noc", StringType()),
    StructField("medal", StringType()),
    StructField("gender", StringType()),
    StructField("pos", StringType())
])

kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", input_topic) \
    .option("startingOffsets", "earliest") \
    .load()

parsed_df = kafka_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# --- 4. Join з біо-даними ---
enriched_df = parsed_df.join(bio_df, on="athlete_id", how="left")

# --- 5. Агрегація ---
aggregated_df = enriched_df \
    .withColumn("timestamp", current_timestamp()) \
    .groupBy("sport", "medal", "gender", "country_noc") \
    .agg(
        avg(col("height").cast("double")).alias("avg_height"),
        avg(col("weight").cast("double")).alias("avg_weight")
    ) \
    .withColumn("timestamp", current_timestamp())

# --- 6. Функція для збереження ---
def foreach_batch_function(batch_df, batch_id):
    # Зберегти в Kafka
    batch_df.selectExpr("to_json(struct(*)) AS value") \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("topic", output_topic) \
        .save()

    # Зберегти в MySQL
    batch_df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", output_table) \
        .option("user", jdbc_user) \
        .option("password", jdbc_password) \
        .mode("append") \
        .save()

# --- 7. Запуск потоку ---
query = aggregated_df.writeStream \
    .foreachBatch(foreach_batch_function) \
    .outputMode("complete") \
    .option("checkpointLocation", "/tmp/spark-checkpoint-athlete") \
    .start()

query.awaitTermination()