package FlinkCommerce;

import FlinkCommerce.dto.Transaction;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import FlinkCommerce.indexer.TransactionDataElasticsearchConnector;
import FlinkCommerce.db.DataStreamJdbcSinkFactory;
import FlinkCommerce.broker.KafkaSourceConfigurator;
import FlinkCommerce.setup.StreamEnvironmentSetup;

/**
 * Main class for the DataStream job in FlinkCommerce.
 * This class is responsible for setting up and executing the data stream processing pipeline
 * that consumes financial transactions from Kafka, processes them, and then sinks the data to
 * JDBC and Elasticsearch.
 */
public class DataStreamJob {
    // Database connection configurations for JDBC
    private static final String JDBC_URL = "jdbc:postgresql://localhost:5432/postgres";
    private static final String USERNAME = "postgres";
    private static final String PASSWORD = "postgres";

    // Kafka Configuration
    private static final String KAFKA_BROKER = "localhost:9092";
    private static final String KAFKA_TOPIC = "financial_transactions";
    private static final String KAFKA_GROUP_ID = "flink-group";
    private static final String KAFKA_SOURCE_NAME = "Kafka source";

    // Elasticsearch Configuration
    private static final String ELASTICSEARCH_HOST = "localhost";
    private static final int ELASTICSEARCH_PORT = 9200;
    private static final String ELASTICSEARCH_SCHEME = "http";
    private static final String ELASTICSEARCH_INDEX = "transactions";

    /**
     * The main method sets up and executes the Flink data stream processing job.
     *
     * @param args Command line arguments (not used).
     * @throws Exception if there is an error in setting up or executing the Flink job.
     */
    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        StreamExecutionEnvironment env = StreamEnvironmentSetup.createEnvironment();

        // Configure Kafka Source
        KafkaSource<Transaction> source = KafkaSourceConfigurator.configureSource(
                KAFKA_BROKER, KAFKA_TOPIC, KAFKA_GROUP_ID);
        DataStream<Transaction> transactionStream = env.fromSource(
                source, WatermarkStrategy.noWatermarks(), KAFKA_SOURCE_NAME);

        // Configure JDBC Sinks
        DataStreamJdbcSinkFactory dataStreamJdbcSinkFactory = new DataStreamJdbcSinkFactory(JDBC_URL, USERNAME, PASSWORD);
        dataStreamJdbcSinkFactory.createTransactionTableSink(transactionStream);
        dataStreamJdbcSinkFactory.createSalesPerCategoryTableSink(transactionStream);
        dataStreamJdbcSinkFactory.createSalesPerDayTableSink(transactionStream);
        dataStreamJdbcSinkFactory.createSalesPerMonthTableSink(transactionStream);
        dataStreamJdbcSinkFactory.insertIntoTransactionsTableSink(transactionStream);
        dataStreamJdbcSinkFactory.insertIntoSalesPerCategoryTableSink(transactionStream);
        dataStreamJdbcSinkFactory.insertIntoSalesPerDayTable(transactionStream);
        dataStreamJdbcSinkFactory.insertIntoSalesPerMonthTable(transactionStream);

        // Configure Elasticsearch Sink
        TransactionDataElasticsearchConnector.createElasticsearchSink(
                ELASTICSEARCH_HOST, ELASTICSEARCH_PORT, ELASTICSEARCH_SCHEME, ELASTICSEARCH_INDEX, transactionStream);

        // Execute the Flink job
        env.execute("Flink Ecommerce Realtime Streaming");
    }
}
