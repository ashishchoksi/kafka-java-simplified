package com.wiki.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;

public class WikiMessageProducer {

    private final static Logger log = LoggerFactory.getLogger(WikiMessageProducer.class);
    private final KafkaProducer<String, String> kafkaProducer;

    /**
     * kafkaProducer<Key, Value>
     *     key: the key you want to send along with message default null
     *     value: the actual message
     */
    public WikiMessageProducer(Properties properties) {
        this.kafkaProducer = new KafkaProducer<>(properties);
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
     * If we have key in our consumer than we can identify duplicate message
     * we can achieve idempotent consumer
     */
    public void sendWithKey(String topic, String key, String message) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, message);
        log.info("writing message to topic: {}, key: {}, message: {}", topic, key, message);
        kafkaProducer.send(producerRecord);
    }

    public void close() {
        kafkaProducer.flush(); // this will wait until message written to kafka
        kafkaProducer.close();
    }
}
