package FlinkCommerce.indexer;

import FlinkCommerce.dto.Transaction;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder;
import org.apache.flink.elasticsearch7.shaded.org.apache.http.HttpHost;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.action.index.IndexRequest;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.client.Requests;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.common.xcontent.XContentType;
import org.apache.flink.streaming.api.datastream.DataStream;

import static FlinkCommerce.util.JsonUtil.convertTransactionToJson;

public class TransactionDataElasticsearchConnector {

    public static void createElasticsearchSink(
            String host,
            int port,
            String scheme,
            String indexName,
            DataStream<Transaction> transactionStream) {

        transactionStream.sinkTo(
                new Elasticsearch7SinkBuilder<Transaction>()
                        .setHosts(new HttpHost(host, port, scheme))
                        .setEmitter((transaction, runtimeContext, requestIndexer) -> {

                            String json = convertTransactionToJson(transaction);

                            IndexRequest indexRequest = Requests.indexRequest()
                                    .index(indexName)
                                    .id(transaction.getTransactionId())
                                    .source(json, XContentType.JSON);
                            requestIndexer.add(indexRequest);
                        })
                        .setBulkFlushMaxActions(2)         // Set maximum number of actions per bulk request
                        .setBulkFlushInterval(1000L)
                        .build()).name("Elasticsearch Sink");
    }

}
