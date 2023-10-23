from kafka import KafkaConsumer
import json

# To consume latest messages and auto-commit offsets
kafka_server = "kafka:9092"
print(kafka_server)
topic ='pricein'
consumer = KafkaConsumer(bootstrap_servers=kafka_server,
                         value_deserializer=json.loads,
                         auto_offset_reset="latest",
                         api_version=(0,11,5)
                         )

consumer.subscribe(topic)

while True:
    data = next(consumer)
    print(data)
    print(data.value)