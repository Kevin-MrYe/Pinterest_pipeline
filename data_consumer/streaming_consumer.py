from kafka import KafkaConsumer
from json import loads

stream_consumer = KafkaConsumer(
    'Pinterest_data',
    bootstrap_servers = "localhost:9092",
    value_deserializer = lambda message: loads(message),
    auto_offset_reset="earliest"
)

for msg in stream_consumer:
    print(msg)
    