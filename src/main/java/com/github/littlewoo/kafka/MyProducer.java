package com.github.littlewoo.kafka;

import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.reactivex.Completable;

@KafkaClient
public interface MyProducer {

    @Topic("my-topic")
    Completable produce(String message);
}
