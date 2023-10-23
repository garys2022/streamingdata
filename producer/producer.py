from kafka import KafkaProducer
import requests
from bs4 import BeautifulSoup as bs
import json
import logging
from time import sleep
from datetime import datetime

kafka_server = "kafka:9092"
print(kafka_server)
topic ='pricein'
producer = KafkaProducer(bootstrap_servers=kafka_server,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                         api_version=(0,11,5)
                         )
print(producer.bootstrap_connected())
print(producer.partitions_for(topic))

def get_response(url="https://www.amazon.co.uk/dp/B00FMBGA3I/"):
    headers={"accept-language": "en-US,en;q=0.9","accept-encoding": "gzip, deflate, br","User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36","accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7"}
    response = requests.get(url,headers=headers)
    return response

def get_title_and_price(soup:str):
    o={}
    try:
        o['title'] = soup.find('h1',{'id':'title'}).text.strip().encode("ascii",'ignore').decode()
        o["price"] = soup.find("span", {"class": "a-price"}).find("span").text.encode("ascii","ignore").decode()
        o['timestamp'] = str(datetime.now())
    except:
        o['title'] = None
        o['price'] = None
        o['timestamp'] = str(datetime.now())
    return o

def push_msg_to_kafka(producer,msg:dict,topic='pricein'):
    future = producer.send(topic,
                  value=msg)
    record_metadata = future.get(timeout=10)
    producer.flush()
    pass

def on_success(record):
    print(record.topic)
    print(record.partition)
    print(record.offset)


if __name__ == '__main__':

    while True:
        #Get info from amazon
        print('start')
        logging.info('Start post request')
        response = get_response()
        logging.info(response.status_code)

        #prepare the message
        soup = bs(response.text, 'html.parser')
        msg_dict = get_title_and_price(response)
        msg = json.dumps(msg_dict, indent=4)

        #send message to kafka
        print(msg)
        record = push_msg_to_kafka(producer,msg=msg)

        print('done')
        sleep(10)