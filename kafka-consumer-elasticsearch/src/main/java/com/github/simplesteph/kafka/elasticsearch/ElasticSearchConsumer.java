package com.github.simplesteph.kafka.elasticsearch;

import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {

    public static RestClient createClient(){

        String hostname = "localhost";

        RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostname, 9200, "http")
        );

        RestClient client = builder.build();

        return client;
    }

    public static void main(String[] args) throws IOException, InterruptedException {

        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
        RestClient client = createClient();



        KafkaConsumer<String, String> consumer = createConsumer("twiter-producer");
        while(true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String, String> record : records){

                // 2 Strategies
                // Kafka generic ID
                //String id = record.topic() + "_" + record.partition() + "_" + record.offset();

                // Twitter feed specific id
                String id = extractIdFromTweet(record.value());

                // Insert data into ElasticSearch
                Request request = new Request(
                        "POST",
                        "/twitter/tweets/"+ id);

                request.setEntity(new NStringEntity(
                        record.value(),
                        ContentType.APPLICATION_JSON));

                Response response = client.performRequest(request);
                String responseBody = EntityUtils.toString(response.getEntity());
                logger.info("\n\n" + responseBody);
                Thread.sleep(20);
            }

        }
        //client.close();

    }

    private static String extractIdFromTweet(String tweetJson) {
        return JsonParser.parseString(tweetJson)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
    }

    public static KafkaConsumer<String, String> createConsumer(String topic){
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "kafka-elasticsearch";

        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(topic));

        return consumer;
    }

}
