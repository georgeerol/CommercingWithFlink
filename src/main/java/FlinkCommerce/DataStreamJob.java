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

/**
 * Main class for the DataStream job in FlinkCommerce.
 * This class is responsible for setting up and executing the data stream processing pipeline
 * that consumes financial transactions from Kafka, processes them, and then sinks the data to
 * JDBC and Elasticsearch.
 */
public class DataStreamJob {
    private static final String JDBC_URL = "jdbc:postgresql://localhost:5432/postgres";
    private static final String USERNAME = "postgres";
    private static final String PASSWORD = "postgres";

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
                "localhost:9092", "financial_transactions", "flink-group");
        DataStream<Transaction> transactionStream = env.fromSource(
                source, WatermarkStrategy.noWatermarks(), "Kafka source");

        // Configure JDBC Sinks
        JdbcSinkConfigurator jdbcSinkConfigurator = new JdbcSinkConfigurator(JDBC_URL, USERNAME, PASSWORD);
        jdbcSinkConfigurator.createTransactionTableSink(transactionStream);
        jdbcSinkConfigurator.createSalesPerCategoryTableSink(transactionStream);
        jdbcSinkConfigurator.createSalesPerDayTableSink(transactionStream);
        jdbcSinkConfigurator.createSalesPerMonthTableSink(transactionStream);
        jdbcSinkConfigurator.insertIntoTransactionsTableSink(transactionStream);
        jdbcSinkConfigurator.insertIntoSalesPerCategoryTableSink(transactionStream);
        jdbcSinkConfigurator.insertIntoSalesPerDayTable(transactionStream);
        jdbcSinkConfigurator.insertIntoSalesPerMonthTable(transactionStream);

        // Configure Elasticsearch Sink
        ElasticsearchSinkConfigurator.createElasticsearchSink(
                "localhost", 9200, "http", "transactions", transactionStream);

        // Execute the Flink job
        env.execute("Flink Ecommerce Realtime Streaming");
    }
}
