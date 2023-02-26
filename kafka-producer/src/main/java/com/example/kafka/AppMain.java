package com.example.kafka;

import com.example.kafka.producer.MyProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class AppMain {

    private static final Logger log = LoggerFactory.getLogger(AppMain.class);

    /**
     * There are multiple ways to write the data
     * 1) write simple data
     * 2) write data with callback
     * 3) write data with key
     */
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        MyProducer kafkaProducer = new MyProducer(properties);

        /**
         * todo:
         *  with below 3 method we can write to kafka
         *  So, the behavior is the same - write to kafka
         *  but the no of parameters are different
         *  so, how you will refactor this code?
         *  how we can apply OOPs here?
         */
        kafkaProducer.send("first_topic", "message_45");
        kafkaProducer.sendAndPrintCallback("first_topic", "message_46");
        kafkaProducer.sendWithMessageKey("first_topic", "k1", "message_47");
    }
}
