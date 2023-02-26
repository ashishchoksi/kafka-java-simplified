package com.example.kafka;

import com.example.kafka.consumer.MyConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

public class AppMain {

    public static void main(String[] args) {
        Properties properties = new Properties();
        String server = "127.0.0.1:9092";
        String consumerGroup = "first_group";
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // none/earliest/latest

        String topic = "first_topic";
        MyConsumer myConsumer = new MyConsumer(properties);
        myConsumer.read(topic);

    }

}
