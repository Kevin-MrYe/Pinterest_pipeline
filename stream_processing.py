import os
from click import option
from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.functions import from_json
from pyspark.sql.functions import when, col, regexp_replace, max
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

json_schema = StructType()\
        .add("unique_id", StringType())\
        .add("index_id", IntegerType())\
        .add("title", StringType())\
        .add("category", StringType())\
        .add("description", StringType())\
        .add("follower_count", StringType())\
        .add("tag_list", StringType())\
        .add("is_image_or_video", StringType())\
        .add("image_src", StringType())\
        .add("downloaded", IntegerType())\
        .add("save_location", StringType())    




# Select the value part of the kafka message and cast it to a string.
stream_df = stream_df\
        .withColumn("value", from_json(stream_df["value"], json_schema))\
        .select(col("value.*"))

# replace error or empty data with Nones
stream_df = stream_df.replace({'User Info Error': None}, subset = ['follower_count','poster_name']) \
                .replace({'No description available Story format': None}, subset = ['description']) \
                .replace({'No description available': None}, subset = ['description']) \
                .replace({'Image src error.': None}, subset = ['image_src'])\
                .replace({'N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e': None}, subset = ['tag_list'])\
                .replace({'Image src error.': None}, subset = ['image_src'])\
                .replace({"No Title Data Available": None}, subset = ['title']) \

# Convert the unit to corresponding zeros
stream_df = stream_df.withColumn("follower_count", when(col('follower_count').like("%k"), regexp_replace('follower_count', 'k', '000')) \
                .when(col('follower_count').like("%M"), regexp_replace('follower_count', 'M', '000000'))\
                .cast("int"))

# Convert the type into int
stream_df= stream_df.withColumn("downloaded", stream_df["downloaded"].cast("int")) \
                .withColumn("index", stream_df["index"].cast("int")) 

# Rename the column
stream_df = stream_df.withColumnRenamed("index", "index_id")

# reorder columns
stream_df = stream_df\
        .select('unique_id',
                'index_id',
                'title',
                'category',
                'description',
                'follower_count',
                'tag_list',
                'is_image_or_video',
                'image_src',
                'downloaded',
                'save_location'
                )

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
