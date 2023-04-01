package com.kafka.opensearch;

import com.kafka.opensearch.service.WikimediaService;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.ActionListener;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Component
@Slf4j
@Data
public class OpenSearchRunner implements CommandLineRunner {


    public static RestHighLevelClient createOpenSearchClient() {
        String connString = "http://localhost:9200";
        //String connString = "https://c9p5mwld41:45zeygn9hy@kafka-course-2322630105.eu-west-1.bonsaisearch.net:443";

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

    public static KafkaConsumer<String,String> createKafkaConsumer(){
        Properties properties = new Properties();

        //Connect to localhost
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // create consumer configs
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", "opensearch-consumer");
        properties.setProperty("auto.offset.reset", "latest");

        return new KafkaConsumer<>(properties);
    }

    @Override
    public void run(String... args) throws Exception {

        // first create an OpenSearch Client
        RestHighLevelClient openSearchClient = createOpenSearchClient();

        //create kafka consumer
        KafkaConsumer<String,String> kafkaConsumer = createKafkaConsumer();

        try(openSearchClient) {

            boolean indexExists = openSearchClient.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);

            if (!indexExists) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                log.info("The Wikimedia Index has been created!");
            } else {
                log.info("The Wikimedia Index already exits");
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        kafkaConsumer.subscribe(Collections.singleton("wikimedia-data"));

        while (true){
            ConsumerRecords<String,String> records = kafkaConsumer.poll(Duration.ofMillis(1000));

            int recordCount = records.count();

            log.info("reciving " + recordCount + " records.");

            for(ConsumerRecord<String,String> record : records){


                IndexRequest indexRequest = new IndexRequest("wikimedia")
                        .source(record.value(), XContentType.JSON);

                openSearchClient.indexAsync(indexRequest, RequestOptions.DEFAULT,
                        new ActionListener<>() {
                            @Override
                            public void onResponse(IndexResponse indexResponse) {
                                log.info("in on indexResponse",indexResponse);
                                log.info("id",indexResponse.getId());

                            }

                            @Override
                            public void onFailure(Exception e) {
                                log.info("in on failure",e);
                            }
                        });


            }

        }
    }
}
