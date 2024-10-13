from kafka import KafkaConsumer
import json
import os

if __name__ == '__main__':
    # To consume latest messages and auto-commit offsets
    kafka_server = os.getenv('KAFKA_ADDRESS')
    topic = os.getenv('KAFKA_SINK_TOPIC')

    print('kafka server', kafka_server)
    print('topic', topic)

    consumer = KafkaConsumer(bootstrap_servers=kafka_server,
                             value_deserializer=json.loads,
                             auto_offset_reset="latest",
                             api_version=(0, 11, 5)
                             )
    print(topic)
    consumer.subscribe(topic)

    while True:
        data = next(consumer)
        print(data)
        print(data.value)
