package com.github.littlewoo;

import com.github.littlewoo.kafka.MyProducer;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import javax.inject.Inject;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@MicronautTest
class MicronautKafkaThreadingTest {

    @Inject
    MyProducer producer;

    private final static Logger log = LoggerFactory.getLogger(MicronautKafkaThreadingTest.class);

    @Test
    void test() throws InterruptedException {
        int i = 0;
        while (i < 50) {
            producer.produce("Hello world! (" + i++ + ")").blockingGet();
        }
        // Give the application some slack
        Thread.sleep(1000);
    }
}
