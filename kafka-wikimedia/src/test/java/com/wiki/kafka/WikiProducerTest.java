package com.wiki.kafka;

import com.wiki.kafka.producer.WikiMessageProducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import java.lang.reflect.Field;
import java.util.Properties;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class WikiProducerTest {

    private KafkaProducer<String, String> kafkaProducer;

    public WikiProducerTest() {
        this.kafkaProducer = Mockito.mock(KafkaProducer.class);
    }

    @Test
    public void testMessageProducer() throws NoSuchFieldException, IllegalAccessException {
        var producer = new WikiMessageProducer(prepareProducerProperties());

        // or we can use FieldUtils from apache comman lang3 to do the same operation
        Field field = producer.getClass().getDeclaredField("kafkaProducer");
        field.setAccessible(true);
        field.set(producer, kafkaProducer);

        producer.send("test_1", "simple message");
        verify(kafkaProducer, times(1)).send(any());
    }

    private Properties prepareProducerProperties() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }

}
