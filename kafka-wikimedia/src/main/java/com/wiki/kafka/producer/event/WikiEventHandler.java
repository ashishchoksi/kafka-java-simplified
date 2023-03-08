package com.wiki.kafka.producer.event;

import com.google.gson.JsonParser;
import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import com.wiki.kafka.producer.WikiMessageProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikiEventHandler implements EventHandler {

    private static final Logger log = LoggerFactory.getLogger(WikiEventHandler.class);
    private final WikiMessageProducer messageProducer;
    private final String topic;

    public WikiEventHandler(WikiMessageProducer messageProducer, String topic) {
        this.messageProducer = messageProducer;
        this.topic = topic;
    }

    @Override
    public void onOpen() throws Exception {

    }

    @Override
    public void onClosed() throws Exception {
        messageProducer.close();
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) throws Exception {
        log.info("Message receive : {}", messageEvent.getData());

        // Send message without key
        // messageProducer.send(topic, messageEvent.getData());

        // Send message with key
        // Its good idea to have a key so that in consumer read same message twice it can identify it
        String key = extractKey(messageEvent.getData());
        messageProducer.sendWithKey(topic, key, messageEvent.getData());
    }

    private String extractKey(String json) {
        // parse the message and send key back with google Gson library
        return JsonParser.parseString(json)
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }

    @Override
    public void onComment(String comment) throws Exception {

    }

    @Override
    public void onError(Throwable t) {

    }
}
