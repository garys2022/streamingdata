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
from datetime import datetime
from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaProducer, KafkaSource,KafkaOffsetsInitializer
from pyflink.datastream.formats.json import JsonRowSerializationSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import Row


def read_from_kafka(env: StreamExecutionEnvironment):
    brokers = os.getenv('KAFKA_ADDRESS')
    read_from_topic = os.getenv('KAFKA_TOPIC')
    sink_to_topic = os.getenv('KAFKA_SINK_TOPIC')
    source = KafkaSource.builder() \
        .set_bootstrap_servers(brokers) \
        .set_topics(read_from_topic) \
        .set_group_id("1") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    ds = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source", type_info=Types.STRING())

    def parse_json(data):
        json_data = json.loads(data)
        items = json_data['items']
        return Row(station_reference=items['stationReference'],
                   river_name=items['riverName'],
                   created_at=datetime.now(),
                   measures=items['measures'])

    def prase_list(data: Row) -> Row:
        for measure in data['measures']:
            yield Row(
                station_reference=data['station_reference'],
                river_name=data['river_name'],
                created_at=data['created_at'],
                notation=measure['notation'],
                parameter_name=measure['parameterName'],
                qualifier=measure['qualifier'],
                value_id=measure['latestReading']['@id'],
                value=measure['latestReading']['value'],
                value_datatime=datetime.strptime(measure['latestReading']['dateTime'], "%Y-%m-%dT%H:%M:%S%z"),
                unitname=measure['unitName']
            )

    types = Types.ROW_NAMED(
            field_names=['station_reference', 'river_name', 'created_at', 'notation', 'parameter_name', 'qualifier',
                         'value_id', 'value', 'value_datatime', 'unitname']
            , field_types=[
                Types.STRING(),
                Types.STRING(),
                Types.SQL_TIMESTAMP(),
                Types.STRING(),
                Types.STRING(),
                Types.STRING(),
                Types.STRING(),
                Types.DOUBLE(),
                Types.SQL_TIMESTAMP(),
                Types.STRING()
            ])

    ds = ds.map(parse_json,
                output_type=Types.ROW(
                    [Types.STRING(), Types.STRING(), Types.SQL_TIMESTAMP(), Types.LIST(Types.STRING())])) \
        .flat_map(prase_list,
                  output_type=types)

    serialization_schema = JsonRowSerializationSchema.builder().with_type_info(
        type_info=types).build()

    kafka_producer = FlinkKafkaProducer(
        topic=sink_to_topic,
        serialization_schema=serialization_schema,
        producer_config={'bootstrap.servers': brokers, 'group.id': 'my-group'})

    ds.add_sink(kafka_producer)
    env.execute('Read from my topic')


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    env = StreamExecutionEnvironment.get_execution_environment()
    # Set Config
    #env.set_parallelism(2)
    #env.add_jars("file:///opt/flink/lib/flink-sql-connector-kafka-3.2.0-1.19.jar")
    #env.add_jars("file:///opt/flink/lib/flink-connector-files-1.19.1.jar")
    #env.add_classpaths("file:///opt/flink/lib/flink-sql-connector-kafka-3.2.0-1.19.jar","file:///opt/flink/lib/flink-connector-files-1.19.1.jar")

    print("start reading data from kafka")
    read_from_kafka(env)
