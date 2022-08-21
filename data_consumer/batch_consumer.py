from imp import IMP_HOOK
import imp
from kafka import KafkaConsumer
import json
import boto3
import tqdm
import os
import yaml

with open('config/s3_creds.yaml','r') as f:
            s3_creds = yaml.safe_load(f)


batch_consumer = KafkaConsumer(
    "Pinterest_data",
    bootstrap_servers = "localhost:9092",
    value_deserializer = lambda message: json.loads(message),
    auto_offset_reset="earliest"
)

s3_client = boto3.client('s3')

for message in batch_consumer:
    print(type(message))
    print(message)
    print((message.value)["unique_id"])
    json_object = json.dumps(message.value, indent=4)
    unique_id = (message.value)["unique_id"]
    filename = 'event-'+unique_id+'.json'
    filepath = os.path.join('events',filename)
    s3_client.put_object(
        Body=json_object,
        Bucket=s3_creds['BUCKET'],
        Key=filepath
    )