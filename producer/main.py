from model import Producer
import producer_func
import os

if __name__ == '__main__':
    kafka_server = "kafka:9092"
    topic = os.getenv('KAFKA_TOPIC')
    print(topic)
    main_producer = Producer(
        server=kafka_server,
        topic=topic,
        get_data_func= producer_func.flood_api_station,
        frequency=50
    )

    main_producer.run()