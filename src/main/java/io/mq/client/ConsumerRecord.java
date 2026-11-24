package io.mq.client;

import io.mq.domain.TopicPartition;

/** attempt is 1 for the first delivery and increases on every redelivery after a nack. */
public record ConsumerRecord(TopicPartition tp, long offset, long timestamp, byte[] key, byte[] value, int attempt) {
    public String valueAsString() { return value == null ? null : new String(value, java.nio.charset.StandardCharsets.UTF_8); }
    public String keyAsString() { return key == null ? null : new String(key, java.nio.charset.StandardCharsets.UTF_8); }
}
