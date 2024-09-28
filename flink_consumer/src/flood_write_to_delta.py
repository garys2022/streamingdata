################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
import logging
import sys
import os
import json
from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaProducer, FlinkKafkaConsumer, KafkaSource, \
    KafkaOffsetsInitializer
from pyflink.datastream.formats.json import JsonRowSerializationSchema, JsonRowDeserializationSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import Row
from datetime import datetime


def read_from_kafka(env: StreamExecutionEnvironment):
    deserialization_schema = JsonRowDeserializationSchema.Builder() \
        .type_info(Types.ROW_NAMED(
        ["@context", "items"],
        [Types.STRING(), Types.STRING()]
    )) \
        .build()

    brokers = 'kafka:9092'
    source = KafkaSource.builder() \
        .set_bootstrap_servers(brokers) \
        .set_topics("mytopic") \
        .set_group_id("my-group") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    ds = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source", type_info=Types.STRING())

    def parse_json(data):
        json_data = json.loads(data)
        items = json_data['items']
        return Row(station_reference=items['stationReference'],
                   river_name=items['riverName'],
                   create_at=datetime.now())
        #return Row(items['stationReference'], items['riverName'], [json.dumps(measure) for measure in items['measures']])

    #ds = ds.map(parse_json, output_type=Types.ROW([Types.STRING(), Types.STRING(), Types.LIST(Types.STRING())]))
    ds = ds.map(parse_json, output_type=Types.ROW([Types.STRING(), Types.STRING(), Types.SQL_TIMESTAMP()]))
    ds.print()
    env.execute()


def write_to_delta(env):
    pass


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    env = StreamExecutionEnvironment.get_execution_environment()
    #env.set_python_requirements('')
    # Set Config
    env.set_parallelism(1)

    #Install dependency
    #env.add_python_archive("venv.zip")
    #env.get_config().set_python_executable("venv.zip/venv/bin/python")
    env.add_jars("file:///opt/flink/lib/flink-sql-connector-kafka-3.2.0-1.19.jar")
    env.add_jars("file:///opt/flink/lib/flink-connector-files-1.19.1.jar")

    print("start reading data from kafka")
    read_from_kafka(env)

    #write_to_delta(env)
