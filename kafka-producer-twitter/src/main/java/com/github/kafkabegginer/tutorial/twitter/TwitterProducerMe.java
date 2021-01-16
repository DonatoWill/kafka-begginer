package com.github.kafkabegginer.tutorial.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterProducerMe {

    private static final String apiKey = "wYIWC2hqQ13wlstDitEtpDJOf";
    private static final String apiKeySecret = "E6NnAplu0spgZurIHTjWMd860ysMhQmiAJYz0zCuChBD46X4a4";
    private static final String bearerToken = "AAAAAAAAAAAAAAAAAAAAAIxB%2FQAAAAAAEadz3qvd2Nu%2FwXjkZddQvIQ4f%2FI%3DdF7Q8EoCkVmnMXzFOcqswq0dG1Mo0swjZleWJg16neBRDUNNzj";

    private static final String accessToken = "1152934496343339011-q8JGDVsII9R5jg6FURElfcGwkT4i1e";
    private static final String accessTokenSecret = "oPsDaxZVptig72eiqrL8JyBvlEw8tyHmXQ4l6lARxStGR";

    public static void main(String[] args) throws InterruptedException, ExecutionException {

        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100);
        BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(100);

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        List<Long> followings = Lists.newArrayList(1234L, 566788L);
        List<String> terms = Lists.newArrayList("twitter", "api");
        hosebirdEndpoint.followings(followings);
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(apiKey, apiKeySecret, accessToken, accessTokenSecret);


        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue))
                .eventMessageQueue(eventQueue);                          // optional: use this if you want to process client events

        Client hosebirdClient = builder.build();
        // Attempts to establish a connection.
        hosebirdClient.connect();

        String bootstrapServers = "127.0.0.1:9092";

        // Create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // Create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        int count = 0;
        // on a different thread, or multiple different threads....
        while (!hosebirdClient.isDone() && count < 10) {
            String msg = msgQueue.take();
            System.out.println("\n\nTwiter message ("+count+")\n");
            System.out.println(msg);
            // create producer record
            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>("twiter-producer", msg);
            producer.send(record).get();
            count++;
        }

        producer.flush();
        producer.close();
      //  hosebirdClient.stop();

    }


}
