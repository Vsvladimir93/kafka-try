package com.example.kafkatry;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Slf4j
public class ConsumerDemoAssignAndSeek {

    public static void main(String[] args) {
        log.info("Consumer demo started...");

        String topic = "first_topic";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // earliest/latest/none

        /* create consumer */

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties)) {


            /*
                assign and seek are mostly used to
                replay data or fetch a specific message
             */

            // assign
            TopicPartition topicPartition = new TopicPartition(topic, 0);
            long offsetToReadFrom = 15L;
            consumer.assign(Collections.singleton(topicPartition));

            // seek
            consumer.seek(topicPartition, offsetToReadFrom);

            int numberOfMessagesToRead = 5;
            int numberOfMessagesRead = 0;
            boolean keepOnReading = true;

//            consumer.subscribe(Collections.singleton(topic));

            while (keepOnReading) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    numberOfMessagesRead++;

                    log.info("Record received - ");
                    log.info("Topic: {}\n" +
                                    "Key: {}\n" +
                                    "Value: {}\n" +
                                    "Offset: {}\n" +
                                    "Partition: {}",
                            consumerRecord.topic(),
                            consumerRecord.key(),
                            consumerRecord.value(),
                            consumerRecord.offset(),
                            consumerRecord.partition()
                    );

                    if (numberOfMessagesToRead <= numberOfMessagesRead) {
                        keepOnReading = false;
                        break;
                    }
                }
            }

            log.info("Exiting");
        }
    }

}
