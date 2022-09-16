from kafka import KafkaConsumer
import json
import boto3
import tqdm
import os
import yaml

# Get S3 bucket credential
with open('config/s3_creds.yaml','r') as f:
            s3_creds = yaml.safe_load(f)


# Create Consumer to consume data from kafka topic
batch_consumer = KafkaConsumer(
    "Pinterest_data",
    bootstrap_servers = "localhost:9092",
    value_deserializer = lambda message: json.loads(message),
    auto_offset_reset="earliest"
)

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