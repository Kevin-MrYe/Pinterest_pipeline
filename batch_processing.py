from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import os
from sqlalchemy import false
import yaml
from pyspark.sql.functions import col
from pyspark.sql.functions import *


# Adding the packages required to get data from S3 and write data to Cassandra
os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages com.amazonaws:aws-java-sdk-s3:1.12.196,org.apache.hadoop:hadoop-aws:3.3.1,com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 pyspark-shell"

class SparkBatch:
    """
    A Class used to do batch processing using pyspark.

    Attributes:
        s3_creds (dict): The dict stores credential for s3 bucket
    """
    def __init__(self):
        """See help(SparkBatch) for details"""

        #Read s3 credentials from yaml
        with open('config/s3_creds.yaml','r') as f:
            self.s3_creds = yaml.safe_load(f)
            print(type(self.s3_creds))

        # Creating our Spark configuration
        conf = SparkConf() \
            .setAppName('S3toSpark') \
            .setMaster('local[*]')


        sc=SparkContext(conf=conf)

        # Configure the setting to read from the S3 bucket
        accessKeyId= self.s3_creds['accessKeyId']
        secretAccessKey= self.s3_creds['secretAccessKey']
        hadoopConf = sc._jsc.hadoopConfiguration()
        hadoopConf.set('fs.s3a.access.key', accessKeyId)
        hadoopConf.set('fs.s3a.secret.key', secretAccessKey)
        hadoopConf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') 
        
        # Create our Spark session
        self.spark=SparkSession(sc)

    def read_from_s3(self):
        """Read data from s3 bucket"""
        # Read from the S3 bucket
        self.df = self.spark.read.option("multiline","true").json(self.s3_creds['BUCKET_NAME']) 

    def transform_data(self):
        """Do transformation on data using pyspark"""

        # replace error or empty data with Nones
        self.df = self.df.replace({'User Info Error': None}, subset = ['follower_count','poster_name']) \
                         .replace({'No description available Story format': None}, subset = ['description']) \
                         .replace({'No description available': None}, subset = ['description']) \
                         .replace({'Image src error.': None}, subset = ['image_src'])\
                         .replace({'N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e': None}, subset = ['tag_list'])\
                         .replace({'Image src error.': None}, subset = ['image_src'])\
                         .replace({"No Title Data Available": None}, subset = ['title']) \
        
        # Convert the unit to corresponding zeros
        self.df = self.df.withColumn("follower_count", when(col('follower_count').like("%k"), regexp_replace('follower_count', 'k', '000')) \
                        .when(col('follower_count').like("%M"), regexp_replace('follower_count', 'M', '000000'))\
                        .cast("int"))

        # Convert the type into int
        self.df= self.df.withColumn("downloaded", self.df["downloaded"].cast("int")) \
                        .withColumn("index", self.df["index"].cast("int")) 
        
        self.df = self.df.withColumnRenamed("index", "index_id")

        # reorders columns
        self.df = self.df.select('unique_id',
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

        self.df.show(500)

    def write_to_cassandra(self):
        """Write the processed data into cassandra database"""

        self.df.write.format("org.apache.spark.sql.cassandra")\
                .option("confirm.truncate","true")\
                .option("spark.cassandra.connection.host","127.0.0.1")\
                .option("spark.cassandra.connection.port", "9042")\
                .option("keyspace","pinterest_data")\
                .option("table","pinterest_batch")\
                .mode("overwrite")\
                .save()

    def run_spark_jobs(self):
        """Run a serie of operations when doing batch processing"""
        self.read_from_s3()
        self.transform_data()
        self.write_to_cassandra()

if __name__ == "__main__":
    spark_ins = SparkBatch()
    spark_ins.run_spark_jobs()


