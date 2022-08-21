# Pinterest Pipeline
Pinterest have billions of user interactions such as image uploads or image clicks which they need to process every day to inform the decisions to make. In this project, I will build the system in the cloud that takes in those events and runs them through two separate pipelines. One for computing real-time metrics (like profile popularity, which would be used to recommend that profile in real-time), and another for computing metrics that depend on historical data (such as the most popular category this year). 

## Table of Contents
* [1.Project Overview](#project-overview)
* [2.Data Ingestion](#data-ingestion)
  * [2.1.Configuring the API](#configuring-the-api)
  * [2.2.Consume data from Kafka](#consume-data-from-kafka)
* [3.Batch Processing](#batch-processing)
  * [3.1.Ingest data into AWS S3](#ingest-data-into-aws-s3)
  * [3.2.Process data using PySpark](#process-data-using-pyspark)
  * [3.3.Send data to Cassandra](#send-data-to-cassandra)
  * [3.4.Run ad-hoc queries using Presto](#run-ad-hoc-queries-using-presto)
  * [3.5.Orchestrate batch processing using Airflow ](#orchestrate-batch-processing-using-airflow)
* [4.Stream Processing](#stream-processing)
* [5.System Monitoring](#system-monitoring)


## Project Overview
As the system overview diagram shown below, this project developed an end-to-end data processing pipeline in Python based on Pinterests experiment processing pipeline.It is implemented based on Lambda architecture to take advantage of both batch and stream-processing. 

Firstly, Creating an API and using Kafka to distribute the data between S3 and Spark streaming. For stream processing, stream data was processed using Spark Streaming and saved to a PostgresSQL database for real-time analysis. For batch processing, batch data was extracted from S3 and transformed in Spark using Airflow to orchestrate the transformations. Then batch data was then loaded into Cassandra for long term storage, ad-hoc analysis using Presto and monitored using Prometheus and Grafana.

<p align="left" width="100%">
  <img src ="https://github.com/Kevin-MrYe/Pinterest_pipeline/blob/master/images/project-overview.png" width = '900px'>
</p>

## Data Ingestion
* ### Configuring the API
To emulate the live environment, the project design an [API](https://github.com/Kevin-MrYe/Pinterest_pipeline/blob/master/api/project_pin_API.py) listening for events made by users on the app, or developer requests to the API. In the mean time, a [user emulations script](https://github.com/Kevin-MrYe/Pinterest_pipeline/blob/master/api/user_posting_emulation.py) was created to simulate users uploading data to Pinterest.

* ### Consume data from Kafka
The project used python-kafka as the client for the Apache Kafka distributed stream processing system.

Create a topic:
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

Send data from API to Kafka:
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


## Batch Processing
### Ingest data into AWS S3
### Process data using PySpark
### Send data to Cassandra
### Run ad-hoc queries using Presto
### Orchestrate batch processing using Airflow 
## Stream Processing
## System Monitoring
