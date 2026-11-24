package io.mq;

import io.mq.config.MqProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(MqProperties.class)
public class MqBrokerApplication {
    public static void main(String[] args) {
        SpringApplication.run(MqBrokerApplication.class, args);
    }
}
