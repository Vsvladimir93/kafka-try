package com.example.kafkatry;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Date;
import java.util.Properties;
import java.util.function.IntFunction;

@Slf4j
public class Main {

    public static void main(String[] args) {
        log.info("Application started successfully...");

        // create Producer properties

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Callback callback = (recordMetadata, e) -> {
            // execute every time a record is successfully sent or an exception is thrown
            if (e == null) {
                log.info("Received new metadata. \n" +
                                "Topic: {}\n" +
                                "Partition: {}\n" +
                                "Offset: {}\n" +
                                "Timestamp: {}",
                        recordMetadata.topic(),
                        recordMetadata.partition(),
                        recordMetadata.offset(),
                        new Date(recordMetadata.timestamp())
                );
            } else {
                log.error("", e);
            }
        };

        // create Producer
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {

            String topic = "first_topic";
            IntFunction<String> key = i -> "id_" + (i % 2 == 0 ? 1 : 2);
            IntFunction<String> value = i -> "Hello world_" + i;

            // create a producer record
            ProducerRecord<String, String> producerRecord;
            for (int i = 0; i < 10; i++) {
                String stringKey = key.apply(i);
                log.info("Producing for key: {}", stringKey);
                producerRecord = new ProducerRecord<>(topic, stringKey, value.apply(i));
                // send data - asynchronous
                producer.send(producerRecord, callback);
                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            log.error("", e);
        }

    }

}