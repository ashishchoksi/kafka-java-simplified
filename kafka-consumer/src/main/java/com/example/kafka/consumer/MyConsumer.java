package com.example.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class MyConsumer {
    private static final Logger log = LoggerFactory.getLogger(MyConsumer.class);
    private final KafkaConsumer<String, String> kafkaConsumer;

    /**
     * create properties config
     * create consumer object
     * subscribe to topic
     * poll from the consumer
     * print the data
     */
    public MyConsumer(Properties properties) {
        kafkaConsumer = new KafkaConsumer<>(properties);
        registerShutDownHook();
    }

    /**
     * Subscribe to the topic
     * Poll the data
     * Print the data
     */
    public void read(String topic) {
        // subscribe to the topic
        //  Topic subscriptions are not incremental. This list will replace the current assignment (if there is one)
        kafkaConsumer.subscribe(Collections.singletonList(topic));

        // poll the record
        while(true) {
            log.info("Polling the message...");

            // this will read all available message (max 500) and if no message is present then wait for 1000ms
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));

            // looping all the message and print
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                log.info("Key : {}, Value: {}", consumerRecord.key(), consumerRecord.value());
            }

            try {
                log.info("Sleep for 5 second...");
                TimeUnit.SECONDS.sleep(5);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

    // close the consumer record...
    private void registerShutDownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Closing the consumer...");
            kafkaConsumer.close();
        }));
    }

}
