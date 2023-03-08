package com.wiki.kafka.producer;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import com.wiki.kafka.producer.event.WikiEventHandler;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * This Class will read data from wikimedia stream
 * and write data to kafka
 */
public class ProducerMain {

    private static final Logger log = LoggerFactory.getLogger(ProducerMain.class);

    public static void main(String[] args) throws InterruptedException {

        // kakfa consumer config
        log.info("preparing kafka producer config...");
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        WikiMessageProducer messageProducer = new WikiMessageProducer(properties);
        String topic = "wiki_message";

        // Event source config
        log.info("preparing kafka stream config...");
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";
        EventHandler eventHandler = new WikiEventHandler(messageProducer, topic);
        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url));
        EventSource eventSource = builder.build();
        eventSource.start();

        // sleep main thread
        TimeUnit.MINUTES.sleep(10);
    }
}
