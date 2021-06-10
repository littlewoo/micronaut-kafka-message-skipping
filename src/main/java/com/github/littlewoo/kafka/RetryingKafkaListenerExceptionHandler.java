package com.github.littlewoo.kafka;

import io.micronaut.configuration.kafka.exceptions.DefaultKafkaListenerExceptionHandler;
import io.micronaut.configuration.kafka.exceptions.KafkaListenerException;
import io.micronaut.configuration.kafka.exceptions.KafkaListenerExceptionHandler;
import io.micronaut.context.annotation.Replaces;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.inject.Singleton;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
@Replaces(DefaultKafkaListenerExceptionHandler.class)
public class RetryingKafkaListenerExceptionHandler implements KafkaListenerExceptionHandler {
    private static final Logger LOG = LoggerFactory.getLogger(RetryingKafkaListenerExceptionHandler.class);

    @Override
    public void handle(final KafkaListenerException exception) {
        final Throwable cause = exception.getCause();
        final Object consumerBean = exception.getKafkaListener();
        Optional<ConsumerRecord<?, ?>> consumerRecord = exception.getConsumerRecord();
        LOG.error("Error processing record [{}] for Kafka consumer [{}] produced error: [{}] on thread {}",
            consumerRecord.map(this::logDetails), consumerBean, cause, Thread.currentThread().getName());
        if (consumerRecord.isPresent()) {
            seekBackToFailedRecordOnException(consumerRecord.get(), exception.getKafkaConsumer());
        } else {
            LOG.error("Kafka consumer [{}] produced error: record not found for listener exception: [{}]",
                consumerBean, cause);
        }
    }

    protected void seekBackToFailedRecordOnException(@Nonnull ConsumerRecord record, @Nonnull Consumer kafkaConsumer) {
        try {
            LOG.warn("Seeking back to failed consumer record for partition {}-{} and offset {}",
                record.topic(), record.partition(), record.offset());
            kafkaConsumer.seek(new TopicPartition(record.topic(), record.partition()), record.offset());
        } catch (IllegalArgumentException | IllegalStateException e) {
            LOG.error(
                "Kafka consumer failed to seek offset to processing exception record: [{}] with error: [{}]",
                record,
                e
            );
        }
    }

    private String logDetails(ConsumerRecord<?, ?> record) {
        return Map.of("topic", record.topic(), "partition", record.partition(), "offset", record.offset())
                   .toString();
    }
}