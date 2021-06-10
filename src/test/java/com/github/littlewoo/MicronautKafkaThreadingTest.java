package com.github.littlewoo;

import com.github.littlewoo.kafka.MyConsumer;
import com.github.littlewoo.kafka.MyProducer;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import javax.inject.Inject;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

@MicronautTest
class MicronautKafkaThreadingTest {

    @Inject
    MyProducer producer;

    @Inject
    MyConsumer consumer;

    @Test
    void test() throws InterruptedException {
        int i = 0;
        while (i < 50) {
            producer.produce("Hello world! (" + i++ + ")").blockingGet();
        }
        // Give the application some slack
        Thread.sleep(1000);
        assertEquals(50, consumer.getMessages().size());
    }
}
