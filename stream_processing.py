import os
from click import option
from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.functions import from_json
from pyspark.sql.functions import col
import yaml

# Download spark sql kakfa package from Maven repository and submit to PySpark at runtime. 
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,org.postgresql:postgresql:42.5.0 pyspark-shell'
# specify the topic we want to stream data from.
kafka_topic_name = "Pinterest_data"
# Specify your Kafka server to read data from.
kafka_bootstrap_servers = 'localhost:9092'

with open('config/postgres_creds.yaml','r') as f:
            ps_creds = yaml.safe_load(f)


spark = SparkSession \
        .builder \
        .appName("KafkaStreaming ") \
        .getOrCreate()

# Only display Error messages in the console.
spark.sparkContext.setLogLevel("ERROR")

# Construct a streaming DataFrame that reads from topic
stream_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
        .option("startingOffsets", "latest") \
        .load()

json_schema = StructType().add("category", StringType()).add("is_image_or_video", StringType())
# Select the value part of the kafka message and cast it to a string.
stream_df = stream_df.select(
        from_json(col("value").cast("string").alias("value"),
        json_schema).alias("parsed_data")).select(col("parsed_data.*"))


#outputing the data to postgresql
def _write_streaming(df, epoch_id) -> None:         

    df.write \
        .mode('append') \
        .format("jdbc") \
        .option("url", ps_creds["URL"]) \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", ps_creds["DBTABLE"]) \
        .option("user", ps_creds["USER"]) \
        .option("password", ps_creds["PASSWORD"]) \
        .save()

stream_df.writeStream \
    .foreachBatch(_write_streaming)\
    .start() \
    .awaitTermination()
