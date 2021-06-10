package com.github.littlewoo.kafka;

import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.context.event.ShutdownEvent;
import io.micronaut.messaging.annotation.MessageBody;
import io.micronaut.runtime.event.annotation.EventListener;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@KafkaListener(groupId = "MyConsumer")
public class MyConsumer {

    private static final Logger log = LoggerFactory.getLogger(MyConsumer.class);

    private final ConcurrentHashMap<Long, String> messages = new ConcurrentHashMap<>();

    @Topic("my-topic")
    public void consumeRaw(@MessageBody String message, long offset) {
        messages.put(offset, message);
        log.info("Reading message {}", message);
        throw new RuntimeException("I can't let you do that");
    }

    @EventListener
    public void onShutdownEvent(ShutdownEvent event) {
        log.info("########### SHUTDOWN ##########");
        log.info("nr messages: {}", messages.size());
        log.info(messages.toString());
        log.info("###############################");
    }

}
