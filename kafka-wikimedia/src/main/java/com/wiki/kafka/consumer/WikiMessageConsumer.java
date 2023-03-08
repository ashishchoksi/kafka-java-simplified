package com.wiki.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikiMessageConsumer {

    private static final Logger log = LoggerFactory.getLogger(WikiMessageConsumer.class);
    private final KafkaConsumer<String, String> kafkaConsumer;

    public WikiMessageConsumer(Properties properties) {
        this.kafkaConsumer = new KafkaConsumer<>(properties);
        registerShutdownHook();
    }

    private void registerShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Executing the shutdownHook to release consumer");
            kafkaConsumer.close();
        }));
    }

    public void read(String topic) {
        kafkaConsumer.subscribe(Collections.singletonList(topic));

        log.info("Start reading the message from topic {} ...", topic);

        while (true) {
            log.info("Polling the message...");

            // This will read all available message (max 500 per call) and if no message present wait for 1000ms
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
            log.info("Poll total {} messages", consumerRecords.count());

            // looping through messages
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                log.info("Reading message with key: {}, value: {}", consumerRecord.key(), consumerRecord.value());
            }

            try {
                log.info("sleeping for 5 seconds");
                TimeUnit.SECONDS.sleep(5);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }

    }
}
