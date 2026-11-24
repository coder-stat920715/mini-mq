package io.mq.config;

import io.mq.broker.GroupCoordinator;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

/** All broker settings, bound from {@code mq.*} (application.yml, env vars such as MQ_PORT, or CLI args). */
@ConfigurationProperties(prefix = "mq")
public record MqProperties(
        @DefaultValue("9092") int port,
        @DefaultValue("./data") String dataDir,
        @DefaultValue("0") int ioThreads,                 // 0 = number of cores
        @DefaultValue("0") int partitionThreads,          // 0 = number of cores
        @DefaultValue("268435456") long segmentBytes,     // 256 MiB
        @DefaultValue("4096") int indexIntervalBytes,
        @DefaultValue("7d") Duration retention,           // time-based retention (0 = keep forever)
        @DefaultValue("-1") long retentionBytes,          // size-based retention per partition (-1 = unlimited)
        @DefaultValue("1s") Duration flushInterval,
        @DefaultValue("1") int minInsyncReplicas,
        @DefaultValue("STICKY") GroupCoordinator.Strategy assignmentStrategy,
        @DefaultValue("4194304") int maxFetchBytes) {
}
