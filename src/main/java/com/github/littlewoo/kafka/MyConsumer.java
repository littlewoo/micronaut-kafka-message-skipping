package com.github.littlewoo.kafka;

import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.OffsetReset;
import io.micronaut.configuration.kafka.annotation.OffsetStrategy;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.core.annotation.Blocking;
import io.reactivex.Completable;
import org.slf4j.LoggerFactory;
import static java.time.Instant.now;

@KafkaListener(groupId = "MyConsumer",
    offsetReset = OffsetReset.EARLIEST,
    offsetStrategy = OffsetStrategy.SYNC_PER_RECORD)
public class MyConsumer {

    @Blocking
    @Topic("my-topic")
    public Completable consumeRaw(String message) {
        return Completable.error(() -> new RuntimeException("I can't let you do that, Dave."))
            .doOnError(e -> LoggerFactory.getLogger(MyConsumer.class).error("An error happened on thread {}: {}",
                Thread.currentThread().getName(), e));
    }
}
