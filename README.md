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

Firstly, Creating an API and using Kafka to distribute the data between S3 and Spark streaming. 

For stream processing, stream data was processed using Structured Streaming and saved to a PostgresSQL database for real-time analysis. 

For batch processing, batch data was extracted from S3 and transformed in Spark using Airflow to orchestrate the transformations. Then batch data was then loaded into Cassandra for long term storage, ad-hoc analysis using Presto and monitored using Prometheus and Grafana.

<p align="left" width="100%">
  <img src ="https://github.com/Kevin-MrYe/Pinterest_pipeline/blob/master/images/project-overview2.png" width = '900px'>
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
# Create Producer to send message to a kafka topic
td_producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    client_id="Pinterest data producer",
    value_serializer=lambda mlmessage: dumps(mlmessage).encode("ascii")
) 

# An API execute some actions when a post request to localhost:9092/pin/
@app.post("/pin/")
def get_db_row(item: Data):
    data = dict(item)
    print(data)
    td_producer.send(topic="Pinterest_data", value=data)
    return item
```



## 3. Batch Processing
### 3.1 Consume data into AWS S3
For batch data, this part will use pyspark to consume data from kafka and store the data into AWS S3. Generally, we can use KafkaConsumer to consume data from kafka, then write data to AWS S3 using boto3. Specific steps are as follows:
1. Consumer data from Kafka
```python
# Create Consumer to consume data from kafka topic
batch_consumer = KafkaConsumer(
    "Pinterest_data",
    bootstrap_servers = "localhost:9092",
    value_deserializer = lambda message: json.loads(message),
    auto_offset_reset="earliest"
)
```

2.  Write data to AWS S3 using boto3
```python
# Write data from Consumer to S3 bucket
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
```

2. Process data using spark SQL

Spark SQL is a Spark module for structured data processing. It provides a series of operations including selection, aggregation, etc. Here are some simple cleaning operations：
```python
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

 # Rename the column
 self.df = self.df.withColumnRenamed("index", "index_id")

 # reorder columns
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
self.df.write.format("org.apache.spark.sql.cassandra")\
                .option("confirm.truncate","true")\
                .option("spark.cassandra.connection.host","127.0.0.1")\
                .option("spark.cassandra.connection.port", "9042")\
                .option("keyspace","pinterest_data")\
                .option("table","pinterest_batch")\
                .mode("overwrite")\
                .save()
```

### 3.4 Run ad-hoc queries using Presto
Presto is a powerful data querying engine that does not provide its own data storage platform. Accordingly, we need to integrate Presto with other tools in order to be able to query data. The core strength of Presto is a feature called data federation, meaning that in a single query, Presto can connect to and combine data from multiple sources.

* Presto-Cassandra Integration
 
In order to connect Presto with Cassandra, we need to create a Presto connector configuration file. Presto looks for connector files in "/etc/catalog/" by default.
```console
cd $PRESTO_HOME/etc
sudo mkdir catalog
```

Then create a new file called **cassandra.properties** which specifying connector name and contact points.
```console
# Create a new cassandra.properties file
sudo nano cassandra.properties

# Add this to the cassandra.properties file
connector.name=cassandra # cassandra is one possible option outlined in docs
cassandra.contact-points=127.0.0.1
```

### 3.5 Orchestrate batch processing using Airflow 
Apache Airflow is a task orchestration tool that allows we to define a series of tasks that are executed in a specific order. In Airflow we use Directed Acyclic Graphs (DAGs) to define a workflow. For batch processing, we usually trigger tasks manually or through the scheduler. In this project's pipeline, we want to execute batche processing once a day. This includes executing **batch_consumer.py** and **batch_processing.py**:
```python
with DAG(dag_id='batch_processing',
         default_args=default_args,
         schedule_interval='0 10 * * *',
         catchup=False,
         ) as dag:

    batch_consume_task = BashOperator(
        task_id='consume_batch_data',
        bash_command=f'cd {work_dir} && python batch_consumer.py '
    )
    batch_process_task = BashOperator(
        task_id='process_batch_data',
        bash_command=f'cd {work_dir} && python batch_processing.py'
    )

    batch_consume_task >> batch_process_task
```


## 4. Stream Processing
For streaming data, this project uses Spark Structured Streaming to consume data from Kafka in real time, then process the data, and finally send the streaming data to PostgreSQL for storage.Similarly, saprk requires appropriate additional libraries (jars) and configuration information to integrate Kafka and PostgreSQL. The specific configuration is as follows:

1. **Consume stream data from kafka**

Add-on library
```
org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1
```

Configuration:
```python
# Construct a streaming DataFrame that read date from topic
stream_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
        .option("startingOffsets", "latest") \
        .load()
```
2. **Write stream data to postgreSQL**

Add-on library
```
org.postgresql:postgresql:42.5.0
```

Configuration:
```python
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
```

## 5. System Monitoring
Finally, I use prometheus to monitor Cassandra and PostgreSQL metrics, and use Grafana to build visual dashboards, which can be observed intuitively.

1. Monitor Cassandra
<p align="left" width="100%">
  <img src ="https://github.com/Kevin-MrYe/Pinterest_pipeline/blob/master/images/cassandra_metrics.png" width = '900px'>
</p>

2. Monitor PostgreSQL
<p align="left" width="100%">
  <img src ="https://github.com/Kevin-MrYe/Pinterest_pipeline/blob/master/images/postgresql_metrics.png" width = '900px'>
</p>
