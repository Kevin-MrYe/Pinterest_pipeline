# Pinterest Pipeline
Pinterest have billions of user interactions such as image uploads or image clicks which they need to process every day to inform the decisions to make. In this project, I will build the system in the cloud that takes in those events and runs them through two separate pipelines. One for computing real-time metrics (like profile popularity, which would be used to recommend that profile in real-time), and another for computing metrics that depend on historical data (such as the most popular category this year). 

## Table of Contents
* [1.Project Overview](#1-project-overview)
* [2.Data Ingestion](#2-data-ingestion)
  * [2.1.Configuring the API](#21-configuring-the-api)
  * [2.2.Send data to Kafka](#22-send-data-to-kafka)
* [3.Batch Processing](#3-batch-processing)
  * [3.1.Consume data into AWS S3](#31-consume-data-into-aws-s3)
  * [3.2.Process data using PySpark](#32-process-data-using-pyspark)
  * [3.3.Send data to Cassandra](#33-send-data-to-cassandra)
  * [3.4.Run ad-hoc queries using Presto](#34-run-ad-hoc-queries-using-presto)
  * [3.5.Orchestrate batch processing using Airflow ](#35-orchestrate-batch-processing-using-airflow)
* [4.Stream Processing](#4-stream-processing)
* [5.System Monitoring](#5-system-monitoring)


## 1. Project Overview
As the system overview diagram shown below, this project developed an end-to-end data processing pipeline in Python based on Pinterests experiment processing pipeline.It is implemented based on Lambda architecture to take advantage of both batch and stream-processing. 

Firstly, Creating an API and using Kafka to distribute the data between S3 and Spark streaming. For stream processing, stream data was processed using Spark Streaming and saved to a PostgresSQL database for real-time analysis. For batch processing, batch data was extracted from S3 and transformed in Spark using Airflow to orchestrate the transformations. Then batch data was then loaded into Cassandra for long term storage, ad-hoc analysis using Presto and monitored using Prometheus and Grafana.

<p align="left" width="100%">
  <img src ="https://github.com/Kevin-MrYe/Pinterest_pipeline/blob/master/images/project-overview.png" width = '900px'>
</p>

## 2. Data Ingestion
### 2.1 Configuring the API
To emulate the live environment, the project design an [API](https://github.com/Kevin-MrYe/Pinterest_pipeline/blob/master/api/project_pin_API.py) listening for events made by users on the app, or developer requests to the API. In the mean time, a [user emulations script](https://github.com/Kevin-MrYe/Pinterest_pipeline/blob/master/api/user_posting_emulation.py) was created to simulate users uploading data to Pinterest.

### 2.2 Send data to Kafka
The project used python-kafka as the client for the Apache Kafka distributed stream processing system.

1. Create a topic:
```python
admin_client = KafkaAdminClient(
    bootstrap_servers="localhost:9092", 
    client_id="Kafka Administrator"
)
# Topics to create must be passed as a list
topics = []
topics.append(NewTopic(name="Pinterest_data", num_partitions=3, replication_factor=1))
admin_client.create_topics(new_topics=topics)
```

2. Send data from API to Kafka:
```python
td_producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    client_id="Pinterest data producer",
    value_serializer=lambda mlmessage: dumps(mlmessage).encode("ascii")
) 

@app.post("/pin/")
def get_db_row(item: Data):
    data = dict(item)
    td_producer.send(topic="Pinterest_data", value=data)
    return item
```



## 3. Batch Processing
### 3.1 Consume data into AWS S3
For batch data, this part will use pyspark to consume data from kafka and store the data into AWS S3. Generally, we can use KafkaConsumer to consume data from kafka, then write data to AWS S3 using boto3. Specific steps are as follows:
1. Consumer data from Kafka
```python
batch_consumer = KafkaConsumer(
    "Pinterest_data",
    bootstrap_servers = "localhost:9092",
    value_deserializer = lambda message: json.loads(message),
    auto_offset_reset="earliest"
)
```

2.  Write data to AWS S3 using boto3
```python
s3_client = boto3.client('s3')
for message in batch_consumer:
    json_object = json.dumps(message.value, indent=4)
    unique_id = (message.value)["unique_id"]
    filename = 'event-'+unique_id+'.json'
    filepath = os.path.join('events',filename)
    s3_client.put_object(
        Body=json_object,
        Bucket=s3_creds['BUCKET'],
        Key=filepath
    )
```
### 3.2 Process data using PySpark
When processing data with Spark, we need to read data from different data sources, such as the popular AWS S3. Therefore, this part first explains how to use pyspark to read data from AWS S3, and then use spark SQL for simple data processing.

1. Read data from AWS S3

In general, we can integrate spark with other applications by adding appropriate additional libraries(jars). These add-on libraries act as connectors. These add-on libraries can be found in the [Maven repo](https://mvnrepository.com/repos/central) and can be imported by adding Maven coordinates or downloading the jar. Integrating spark with AWS S3 requires the following two dependent libraries:
```
com.amazonaws:aws-java-sdk-s3:1.12.196
org.apache.hadoop:hadoop-aws:3.3.1
```
(Versions depend on your case)

After configuring the connector, you need to configure the context (access ID and access key) in SparkSession for authentication.
```python
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
df = spark.read.option("multiline"t5,"true").json(s3_creds['BUCKET_NAME']) 
# You may want to change this to read csv depending on the files your reading from the bucket
```

2. Process data using spark SQL

Spark SQL is a Spark module for structured data processing. It provides a series of operations including selection, aggregation, etc. Here are some simple cleaning operations：
```python
# group by category
category_count = df.groupby("category").count().persist()

## filter by category
category_filter = df.filter("category == 'christmas'")

## group by type
type_count = df.groupby("is_image_or_video").count().persist()
```

### 3.3 Send data to Cassandra
Apache Cassandra is one of the most popular NoSQL columnar-based data stores used by global companies today. Cassandra provides aggregation commands such as SUM, COUNT, MAX, MIN, and AVG. But so far, Cassandra doesn't allow JOIN operations on tables. After the batch data is cleaned, it will be sent to Cassandra for storage. Also, integrating spark with Cassandra requires additional connector and environment configuration. Specific information is as follows:

1. Connector(jar)
```
com.datastax.spark:spark-cassandra-connector_2.12:3.2.0
```
(Versions depend on your case)

2. Configuration
```python
conf = SparkConf() \
    .setAppName('S3toSpark') \
    .setMaster('local[*]')\
    .set("spark.cassandra.connection.host", "127.0.0.1")\
    .set("spark.cassandra.connection.port", "9042")
```

### 3.4 Run ad-hoc queries using Presto
Presto is a powerful data querying engine that does not provide its own data storage platform. Accordingly, we need to integrate Presto with other tools in order to be able to query data. 

### 3.5 Orchestrate batch processing using Airflow 
## 4. Stream Processing
## 5. System Monitoring
