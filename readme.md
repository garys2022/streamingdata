# Data Streaming Pipeline

This project demonstrates a data streaming pipeline using a Python container as a producer to fetch data from an
external API, publish it to Apache Kafka, and then use Apache Flink to consume messages from Kafka and store them into a
Delta table.

## Project Overview

The pipeline consists of the following steps:

1. Python Producer: A Python container fetches data from an API and sends the messages to a Kafka topic.
2. Kafka Broker: Acts as a messaging queue, where the producer sends the API data.
3. Flink Consumer - Parse response: Apache Flink reads the messages from the Kafka topic, parse the API response and
   write back to Kafka
4. Flink Consumer - Flink to reads the parsed message from kafka and write to delta tableWrite to Delta writes the
   output to a Delta Lake table for further analysis and persistence.

# Architecture

![Project Architecture](./images/readme/Architecture.jpeg)

## Key Technologies

* Python: Used to fetch data from the API.
* Docker: All components ,except for storage layers, runs inside a Docker container, this enable
* Apache Kafka: Acts as a distributed streaming platform for storing and forwarding messages.
* Apache Flink: Processes the data from Kafka and writes it to Delta Lake.
* Delta Lake: A storage layer that brings reliability to data lakes for both batch and streaming data.

# Getting Started

## Prerequisites

To run this project, ensure you have the following software installed on your system:

* Docker
* Docker Compose
* Maven

## Setup and Installation

1. Clone the Repository:

```commandline
git clone https://github.com/garys2022/streamingdata.git
cd your-repo
```

2. Build image
   prepare the image that will be used in docker-compose
   in flink_consumer, download the flink-sql-connector-kafka-3.2.0-1.19.jar from maven in
   this [link](https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.19/flink-sql-connector-kafka-3.2.0-1.19.jar)

* python producer

```commandline
cd producer
docker build -t producer .
```

* flink
  Go to flink repository

```commandline
cd ..
cd flink_consumer
```

build image

```commandline
docker build -t pyflink .
```

3. Set up environment variable
   Before running the docker compose , set up environment variable by setting value in .env file
   a .envtemplate is provided in this repo , you can rename that to .env and set up the variable accordingly

4. Build jar file for write to delta job
   Java is used for the flink job to write data to delta table
   go to flink_consumer/myartifact and build the jar file

```commandline
cd myartifact
mvn package
```

After running this , you shall found a myartifact-0.1.jar file appear in flink_consumer/myartifact/target
For convenient , this folder will be shared to flink via docker volume to pass the jar file to flink.
alternatively you can re-build the image to add this file in the container instead rather than using docker-compose

5. run the whole system by docker compose
   back to the main repository (streaming data) and run the docker-compose setup

```commandline
cd ..
cd ..
docker-compose up -d
```

6. go to flink job manager container and submit the job to flink

```commandline
./bin/flink run \
  --detached \
	--python usrlib/artifacts/python/prase_api.py \
	--jarfile ./lib/flink-sql-connector-kafka-3.2.0-1.19.jar

./bin/flink run \
      --detached \
      ./usrlib/artifacts/java/myartifact-0.1.jar
```

7. After you have submitted the job, the whole system should be up and running now.
8. Verify the result
   You may run this python script to verify that the delta-table is constantly updating
   Back to the repository and run the testing script

```commandline
cd test
python test.py
```

You shall see an example result like this indicating data is constantly updated to the delta table

```commandline
Delta Table last updated at  2024-10-13 19:48:31
Reading data from version 47
time: 2024-10-13 19:50:06.912393
shape of delta_table (188, 10)



Delta Table last updated at  2024-10-13 19:50:11
Reading data from version 48
time: 2024-10-13 19:51:06.970628
shape of delta_table (192, 10)



Delta Table last updated at  2024-10-13 19:51:51
Reading data from version 49
time: 2024-10-13 19:52:07.025151
shape of delta_table (196, 10)
```