from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import os
import yaml
from sqlalchemy import table, true


# Adding the packages required to get data from S3  
os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages com.amazonaws:aws-java-sdk-s3:1.12.196,org.apache.hadoop:hadoop-aws:3.3.1,com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 pyspark-shell"
with open('config/s3_creds.yaml','r') as f:
            s3_creds = yaml.safe_load(f)
# Creating our Spark configuration
conf = SparkConf() \
    .setAppName('S3toSpark') \
    .setMaster('local[*]')\
    .set("spark.cassandra.connection.host", "127.0.0.1")\
    .set("spark.cassandra.connection.port", "9042")

sc=SparkContext(conf=conf)

# Configure the setting to read from the S3 bucket
accessKeyId= s3_creds['accessKeyId']
secretAccessKey= s3_creds['secretAccessKey']
hadoopConf = sc._jsc.hadoopConfiguration()
hadoopConf.set('fs.s3a.access.key', accessKeyId)
hadoopConf.set('fs.s3a.secret.key', secretAccessKey)
# Allows the package to authenticate with AWS
hadoopConf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') 
# Create our Spark session
spark=SparkSession(sc)
# Read from the S3 bucket
df = spark.read.option("multiline","true").json(s3_creds['BUCKET_NAME']) 
# You may want to change this to read csv depending on the files your reading from the bucket


# group by category
category_count = df.groupby("category").count().persist()
category_count.show()

## filter by category
category_filter = df.filter("category == 'christmas'")
category_filter.show()

## group by type
type_count = df.groupby("is_image_or_video").count().persist()
type_count.show()

category_count.write.format("org.apache.spark.sql.cassandra")\
        .options(table="category_count", keyspace="pinterest_data")\
        .option("confirm.truncate","true")\
        .mode("overwrite")\
        .save()

