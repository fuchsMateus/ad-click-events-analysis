package fuchs;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import java.io.InputStream;

public class Main {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Checkpointing configuration for fault tolerance
        env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setCheckpointStorage("s3://click-event-analysis/flink-checkpoints");
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // Avro schema configuration
        InputStream avroSchemaStream = Main.class.getClassLoader().getResourceAsStream("avro/click_event.avsc");
        Schema schema = new Schema.Parser().parse(avroSchemaStream);

        // Kafka Source configuration
        KafkaSource<GenericRecord> kafkaSource = KafkaSource.<GenericRecord>builder()
                .setBootstrapServers("kafka:9093")
                .setGroupId("flink-click-event-group")
                .setTopics("click_events")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(AvroDeserializationSchema.forGeneric(schema))
                .build();

        // Creating the DataStream from the Kafka Source
        DataStream<GenericRecord> stream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .setParallelism(4);

        // Stream for Most Clicked Categories by Region
        DataStream<CategoryRegion> categoryRegionStream = stream
                .map(event -> new CategoryRegion(
                        event.get("location").toString(),
                        event.get("category").toString(),
                        1L // Inicia com um clique
                ))
                .keyBy(categoryRegion -> categoryRegion.getLocation() + "|" + categoryRegion.getCategory())
                .window(ProcessingTimeSessionWindows.withGap(Time.minutes(1)))
                .reduce((value1, value2) -> {
                    return new CategoryRegion(
                            value1.getLocation(),
                            value1.getCategory(),
                            value1.getClickCount() + value2.getClickCount()
                    );
                });

        // Stream for Most Clicked Categories by Platform
        DataStream<CategoryPlatform> categoryPlatformStream = stream
                .map(event -> new CategoryPlatform(
                        event.get("platform").toString(),
                        event.get("category").toString(),
                        1L
                ))
                .keyBy(categoryPlatform -> categoryPlatform.getPlatform() + "|" + categoryPlatform.getCategory())
                .window(ProcessingTimeSessionWindows.withGap(Time.minutes(1)))
                .reduce((value1, value2) -> {
                    return new CategoryPlatform(
                            value1.getPlatform(),
                            value1.getCategory(),
                            value1.getClickCount() + value2.getClickCount()
                    );
                });

        // Sink for Most Clicked Categories by Region
        categoryRegionStream.sinkTo(
                FileSink
                        .forBulkFormat(
                                new Path("s3://click-event-analysis/output/category_clicks_by_location"),
                                ParquetAvroWriters.forReflectRecord(CategoryRegion.class)
                        )
                        .withRollingPolicy(OnCheckpointRollingPolicy.build())
                        .withOutputFileConfig(
                                OutputFileConfig
                                        .builder()
                                        .withPartPrefix("part")
                                        .withPartSuffix(".parquet")
                                        .build()
                        )
                        .build()
        ).name("Category Clicks by Location");

        // Sink for Most Clicked Categories by Platform
        categoryPlatformStream.sinkTo(FileSink
                .forBulkFormat(new Path("s3://click-event-analysis/output/category_clicks_by_platform"),
                        ParquetAvroWriters.forReflectRecord(CategoryPlatform.class))
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .withOutputFileConfig(
                        OutputFileConfig
                                .builder()
                                .withPartPrefix("part")
                                .withPartSuffix(".parquet")
                                .build()
                )
                .build()).name("Category Clicks by Platform");

        env.execute("FlinkClickEventConsumer");
    }
}
