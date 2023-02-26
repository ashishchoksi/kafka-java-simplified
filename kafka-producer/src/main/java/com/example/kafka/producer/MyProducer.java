package com.example.kafka.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Producer configuration
 * 1) create property class with producer config
 * 2) create producer object
 * 3) create producerRecord
 * 4) write message
 * 5) flush/close producer
 */
public class MyProducer {

    private final static Logger log = LoggerFactory.getLogger(MyProducer.class);
    private final KafkaProducer<String, String> kafkaProducer;

    /**
     * kafkaProducer<Key, Value>
     *     key: the key you want to send along with message default null
     *     value: the actual message
     */
    public MyProducer(Properties properties) {
        this.kafkaProducer = new KafkaProducer<>(properties);
        registerShutdownHook();
    }

    /**
     * ProducerRecord<Key, Value>
     *     key: the key you want to send along with message default null
     *     value: the actual message
     * Even if we have producer configured with <string, string> we still have to write it in producerRecord
     */
    public void send(String topic, String message) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, message);
        log.info("writing message to topic: {}, message: {}", topic, message);
        kafkaProducer.send(producerRecord);
    }

    /**
     * This can write the message and print meta info once write is successful
     */
    public void sendAndPrintCallback(String topic, String message) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, message);
        log.info("writing message to topic: {}, message: {}", topic, message);
        kafkaProducer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                log.info("Getting the callback after writing message: {}", metadata);
            }
        });
    }

    /**
     * message with same key will land into same partition
     */
    public void sendWithMessageKey(String topic, String key, String message) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, message);
        log.info("writing message to topic: {}, key: {} message: {}", topic, key, message);
        kafkaProducer.send(producerRecord);
    }

    private void registerShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("flush and close the producer");
            kafkaProducer.flush(); // this will wait until message written to kafka
            kafkaProducer.close();
        }));
    }

}
