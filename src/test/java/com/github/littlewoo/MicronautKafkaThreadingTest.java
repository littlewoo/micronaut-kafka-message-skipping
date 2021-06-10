package com.github.littlewoo;

import com.github.littlewoo.kafka.MyProducer;
import io.micronaut.runtime.EmbeddedApplication;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;

import javax.inject.Inject;

@MicronautTest
class MicronautKafkaThreadingTest {

    @Inject
    MyProducer producer;

    @Test
    void test() {
        int i=0;
        while (true) {
            producer.produce("Hello world! (" + i + ")").blockingGet();
        }
    }
}
