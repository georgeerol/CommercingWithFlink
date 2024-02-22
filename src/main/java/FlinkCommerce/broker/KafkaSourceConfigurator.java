package FlinkCommerce.broker;

import FlinkCommerce.util.JSONValueDeserializationSchema;
import FlinkCommerce.dto.Transaction;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

public class KafkaSourceConfigurator {
    public static KafkaSource<Transaction> configureSource(String bootstrapServers, String topic, String groupId) {
        return KafkaSource.<Transaction>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new JSONValueDeserializationSchema())
                .build();
    }
}