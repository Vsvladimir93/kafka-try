package com.example.kafkatry;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

@Slf4j
public class ConsumerDemoThreads {

    public static void main(String[] args) {
        log.info("Consumer demo started...");

        String topic = "first_topic";
        String groupId = "consumerGroupId_2";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // earliest/latest/none

        /* create consumer */

        CountDownLatch latch = new CountDownLatch(1);

        ConsumerThread myConsumerThread = new ConsumerThread(
                latch,
                properties,
                topic
        );

        new Thread(myConsumerThread).start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Caught shutdown hook");
            myConsumerThread.shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            log.error("Application was interrupted..");
        } finally {
            log.info("Application is closing");
        }
    }

    @Slf4j
    public static class ConsumerThread implements Runnable {

        private final CountDownLatch latch;
        private final String topic;
        private KafkaConsumer<String, String> consumer;

        public ConsumerThread(CountDownLatch latch, Properties properties, String topic) {
            this.latch = latch;
            this.topic = topic;
            this.consumer = new KafkaConsumer<>(properties);
        }

        @Override
        public void run() {
            try {

                consumer.subscribe(Collections.<String>singleton(topic));

                while (true) {
                    ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
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
            } catch(WakeupException e) {
                log.info("Received shutdown signal!");
            } finally {
                consumer.close();
                latch.countDown();
            }
        }

        public void shutdown() {
            consumer.wakeup();
        }

    }

}
