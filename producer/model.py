from kafka import KafkaProducer
from typing import Callable
import json
import time
import logging

class Producer:
    def __init__(self,server:str,topic:str,get_data_func:Callable,frequency:int):
        """
        The Kafka Producer Client
        :param server: server name of the kafka server
        :param topic: name of the kafka topic
        :param get_data_func: Callable to get the data , this Callable shall return str or json
        :param frequency: Frequency for the producer to extract data in second.
        """
        self.server = server
        self.topic = topic
        self.get_data_func = get_data_func
        self.producer = KafkaProducer(bootstrap_servers=self.server,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                         api_version=(0,11,5)
                         )
        self.frequency = frequency
        self.logger = logging.getLogger(__name__)

    def send(self,msg):
        self.producer.send(
            topic=self.topic,
            value=msg
            )
        self.producer.flush()

    def run(self):
        while True:
            print('loop started')
            msg = self.get_data_func()
            print(msg)
            self.logger.info("Response received")
            self.send(msg)
            self.logger.info("Message Sent")
            time.sleep(self.frequency)
