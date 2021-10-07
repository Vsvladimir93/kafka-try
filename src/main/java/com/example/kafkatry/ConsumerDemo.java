package com.example.kafkatry;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Slf4j
public class ConsumerDemo {

    public static void main(String[] args) {
        log.info("Consumer demo started...");

        String topic = "first_topic";
        String groupId = "consumerGroupId";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // earliest/latest/none

        /* create consumer */

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties)) {

            consumer.subscribe(Collections.singleton(topic));

            outer:
            while (true) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord consumerRecord : consumerRecords) {
                    if (consumerRecord.value().equals("exit")) {
                        break outer;
                    }
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
                }
            }
        }
    }

}
