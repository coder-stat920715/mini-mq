package io.mq.config;

import io.mq.broker.Broker;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.nio.file.Path;

@Configuration(proxyBeanMethods = false)
public class BrokerConfig {

    /** Started after the context is ready (initMethod) and closed on shutdown, flushing all logs. */
    @Bean(initMethod = "start", destroyMethod = "close")
    public Broker broker(MqProperties p) throws IOException {
        int cores = Runtime.getRuntime().availableProcessors();
        return new Broker(new Broker.Config(
                Path.of(p.dataDir()),
                p.port(),
                p.ioThreads() > 0 ? p.ioThreads() : Math.max(2, cores),
                p.partitionThreads() > 0 ? p.partitionThreads() : Math.max(2, cores),
                p.segmentBytes(),
                p.indexIntervalBytes(),
                p.retention().toMillis(),
                p.retentionBytes(),
                Math.max(100, p.flushInterval().toMillis()),
                p.minInsyncReplicas(),
                p.assignmentStrategy(),
                p.maxFetchBytes()));
    }
}
