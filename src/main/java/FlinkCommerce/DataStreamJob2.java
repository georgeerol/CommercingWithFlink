package FlinkCommerce;

import FlinkCommerce.dto.Transaction;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import FlinkCommerce.indexer.ElasticsearchSinkConfigurator;
import FlinkCommerce.db.JdbcSinkConfigurator;
import FlinkCommerce.broker.KafkaSourceConfigurator;
import FlinkCommerce.setup.StreamEnvironmentSetup;

public class DataStreamJob2 {
    private static final String JDBC_URL = "jdbc:postgresql://localhost:5432/postgres";
    private static final String USERNAME = "postgres";
    private static final String PASSWORD = "postgres";

    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        StreamEnvironmentSetup environmentSetup = new StreamEnvironmentSetup();
        StreamExecutionEnvironment env = environmentSetup.createEnvironment();

        // Configure Kafka Source
        KafkaSourceConfigurator sourceConfigurator = new KafkaSourceConfigurator();
        KafkaSource<Transaction> source = sourceConfigurator.configureSource("localhost:9092", "financial_transactions", "flink-group");
        DataStream<Transaction> transactionStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka source");

        // Configure JDBC Sinks
        JdbcSinkConfigurator<Transaction> jdbcSinkConfigurator = new JdbcSinkConfigurator<>(JDBC_URL, USERNAME, PASSWORD, transactionStream);

        String esHost = "localhost";
        int esPort = 9200;
        String esScheme = "http";
        String esIndex = "transactions";
        ElasticsearchSinkConfigurator elasticsearchSinkConfigurator = new ElasticsearchSinkConfigurator();
        elasticsearchSinkConfigurator.createElasticsearchSink(esHost,esPort,esScheme,esIndex,transactionStream);


        // Execute the Flink job
        env.execute("Flink Ecommerce Realtime Streaming");
    }
}
