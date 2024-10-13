package sink.delta;
import java.io.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.flink.sink.DeltaSink;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.fs.Path;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.sql.Timestamp;

public class KafkaToDelta {
    public static void main(String[] args) throws Exception {

        // 1. Initialize Flink streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. Read configuration parameters
        ParameterTool params = ParameterTool.fromArgs(args);
        String kafkaServers = params.get("KAFKA_ADDRESS", "kafka:9092");
        String kafkaTopic = params.get("KAFKA_SINK_TOPIC", "sink");
        String deltaTablePath = params.get("delta-table-path", "/opt/flink/usrlib/data");

        System.out.println(kafkaServers);
        System.out.println(kafkaTopic);
        System.out.println(deltaTablePath);
        // 3. Define the schema using RowType
        RowType rowType = defineRowType();

        // 4. Create Kafka source with the custom deserializer
        KafkaSource<RowData> kafkaSource = KafkaSource.<RowData>builder()
            .setBootstrapServers(kafkaServers)
            .setTopics(kafkaTopic)
            .setStartingOffsets(OffsetsInitializer.earliest()) // Start reading from earliest offsets
            .setValueOnlyDeserializer(
                new JsonRowDataDeserializer(rowType) // Custom deserializer
            //new SimpleStringSchema()
            ) 
            .build();

        // 6. Create the DataStream from Kafka source and write to Delta Lake
        //env.add(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source").print();
        DataStream<RowData> kafkaStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");
        kafkaStream.print();
        createDeltaSink(kafkaStream,deltaTablePath,rowType);
        env.enableCheckpointing(100000);

        // 7. Execute the job
        env.execute("Write To Delta");
    }

    // Method to define RowType (schema)
    public static RowType defineRowType() {
        return RowType.of(
            new VarCharType(255),         // station_reference (STRING)
            new VarCharType(255),         // river_name (STRING)
            new VarCharType(255),        // created_at (SQL_TIMESTAMP)
            new VarCharType(255),         // notation (STRING)
            new VarCharType(255),         // parameter_name (STRING)
            new VarCharType(255),         // qualifier (STRING)
            new VarCharType(255),         // value_id (STRING)
            new DoubleType(),             // value (DOUBLE)
            new VarCharType(255),        // value_datatime (SQL_TIMESTAMP)
            new VarCharType(255)          // unitname (STRING)
        );
    }

    // Custom deserializer class for Kafka messages
    static class JsonRowDataDeserializer implements DeserializationSchema<RowData> {

        private final RowType rowType;
        private final ObjectMapper objectMapper = new ObjectMapper(); // Jackson JSON parser

        public JsonRowDataDeserializer(RowType rowType) {
            this.rowType = rowType;
        }

        @Override
        public RowData deserialize(byte[] record) throws IOException {
            // Deserialize the value (message) of the Kafka record as JSON
            JsonNode jsonNode = objectMapper.readTree(record);

            // Create a new GenericRowData with the number of fields matching the schema
            GenericRowData rowData = new GenericRowData(rowType.getFieldCount());

            // Map the JSON fields to RowData fields (based on the provided schema)
            rowData.setField(0, StringData.fromString(
                jsonNode.has("station_reference") ? jsonNode.get("station_reference").asText() : null)
            );
            rowData.setField(1, 
                StringData.fromString(jsonNode.has("river_name") ? jsonNode.get("river_name").asText() : null));
            rowData.setField(2,
                StringData.fromString(jsonNode.has("created_at") ? jsonNode.get("created_at").asText() : null));
            rowData.setField(3,
                StringData.fromString(jsonNode.has("notation") ? jsonNode.get("notation").asText() : null));
            rowData.setField(4,
                StringData.fromString(jsonNode.has("parameter_name") ? jsonNode.get("parameter_name").asText() : null));
            rowData.setField(5,
                StringData.fromString(jsonNode.has("qualifier") ? jsonNode.get("qualifier").asText() : null));
            rowData.setField(6,
                StringData.fromString(jsonNode.has("value_id") ? jsonNode.get("value_id").asText() : null));
            rowData.setField(7,jsonNode.has("value") ? jsonNode.get("value").asDouble() : null);
            rowData.setField(8,
                StringData.fromString(jsonNode.has("value_datatime") ? jsonNode.get("value_datatime").asText() : null));
            rowData.setField(9,
                StringData.fromString(jsonNode.has("unitname") ? jsonNode.get("unitname").asText() : null));
            return rowData;
        }

        @Override
        public TypeInformation<RowData> getProducedType() {
            // Return the type information for RowData
            return TypeInformation.of(RowData.class);
        }

        @Override
        public boolean isEndOfStream(RowData nextElement) {
            return false;
        }
    }

    // Method to create the Delta Sink
    public static DataStream<RowData> createDeltaSink(DataStream<RowData> stream,
                                               String deltaTablePath,
                                               RowType rowType) {
        DeltaSink<RowData> deltaSink = DeltaSink
            .forRowData(
                new Path(deltaTablePath),
                new Configuration(),
                rowType)
            .build();
        stream.sinkTo(deltaSink);
        return stream;
    }
}
