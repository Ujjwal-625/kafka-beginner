package io.conduktor.demos.kafka.opensearch;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.logging.Logger;

public class OpenSearchConsumer {

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class);

    public static RestHighLevelClient createOpenSearchClient() {
        String connString = "http://localhost:9200";
//        String connString = "https://c9p5mwld41:45zeygn9hy@kafka-course-2322630105.eu-west-1.bonsaisearch.net:443";

        // we build a URI from the connection string
        RestHighLevelClient restHighLevelClient;
        URI connUri = URI.create(connString);
        // extract login information if it exists
        String userInfo = connUri.getUserInfo();
        if (userInfo == null) {
            // REST client without security
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));

        } else {
            // REST client with security
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));


        }

        return restHighLevelClient;
    }

    private static KafkaConsumer<String, String> createKafkaConsumer(){

        String groupId = "consumer-opensearch-demo";

        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // create consumer
        return new KafkaConsumer<>(properties);

    }

    private static String extractId(String json){
        return JsonParser.parseString(json)
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }

    public static void main(String[] args) throws IOException {

        Logger logger = Logger.getLogger(OpenSearchConsumer.class.getName());

        RestHighLevelClient openSearchClient = OpenSearchConsumer.createOpenSearchClient();

        KafkaConsumer<String, String> kafkaConsumer = createKafkaConsumer();

        //we need to create index on openSearch if it is not created
        try(openSearchClient ; kafkaConsumer){
            if(!openSearchClient.indices().exists(new GetIndexRequest("wikimedia") , RequestOptions.DEFAULT )){
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                logger.info("Index created");
            }
            else {
                logger.info("Index already exists");
            }

            kafkaConsumer.subscribe(Collections.singletonList("wikimedia.recentChanges"));

            while(true){
                ConsumerRecords<String,String> record =kafkaConsumer.poll(Duration.ofMillis(3000));

                int recordCount = record.count();
                logger.info(recordCount + " records received");

                // now we will use batching using bulkrequest

                BulkRequest bulkRequest = new BulkRequest();

                for (ConsumerRecord<String, String> record1 : record) {
                    try{
                        String id = extractId(record1.value());

                        IndexRequest indexRequest = new IndexRequest("wikimedia")
                                .source(record1.value(), XContentType.JSON)
                                .id(id);
                        //removing this due to bulkRequest
//                        IndexResponse response = openSearchClient.index(indexRequest,RequestOptions.DEFAULT);
//                        logger.info(response.getId()+"This is Id");

                        bulkRequest.add(indexRequest);

                    }catch (Exception e){

                    }
                }

                // now after batch is ready
                if(bulkRequest.numberOfActions()>0)
                {
                    BulkResponse bulkResponse= openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    logger.info("Inserted "+ bulkResponse.getItems().length + " records");

                    //making some dealy to increase the chances of getting bulk upload

                    try{
                        Thread.sleep(20000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    //commit the offsets after messages are consumed . after processing all the records now we will commit the offsets
                    kafkaConsumer.commitSync();
                    logger.info("offsets are now Commited");
                    //offsets should only be commited only if we are doing bulk Request

                }



            }

        }


    }
}
