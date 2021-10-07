package com.example.kafka_stream_pet;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.example.ApplicationProperties.ProjectProperties;

@Slf4j
public class TwitterProducer {

    public static void main(String[] args) throws Exception {
        new TwitterProducer().run();
    }

    public void run() {
        List<String> terms = List.of("kafka");
        BlockingQueue<String> queue = new LinkedBlockingQueue<>(100_000);

        // create a twitter client
        Client client = createTwitterClient(queue, terms);

        client.connect();

        // create a kafka producer

        // loop to send tweets to kafka

        while(!client.isDone()) {
            String msg = null;
            try {
                msg = queue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                log.error("", e);
                client.stop();
            }
            if (msg != null) {
                log.info("Message: {}", msg);
            }
        }
    }

    private Client createTwitterClient(BlockingQueue<String> queue, List<String> terms) {
        Objects.requireNonNull(queue);
        Objects.requireNonNull(terms);

        Hosts hosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();

        endpoint.trackTerms(terms);

        Authentication auth =
                new OAuth1(
                        ProjectProperties.TWITTER_CONSUMER_API_KEY.get(),
                        ProjectProperties.TWITTER_CONSUMER_API_KEY_SECRET.get(),
                        ProjectProperties.TWITTER_AUTHENTICATION_ACCESS.get(),
                        ProjectProperties.TWITTER_AUTHENTICATION_ACCESS_SECRET.get()
                );

        return new ClientBuilder()
                .name("Twitter Client")
                .hosts(hosts)
                .authentication(auth)
                .endpoint(endpoint)
                .processor(new StringDelimitedProcessor(queue))
                .build();
    }

}
