package ru.yandex.practicum.telemetry.aggregator.configuration;

import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Getter
@Setter
@ConfigurationProperties(prefix = "aggregator.kafka.topics")
@Configuration
@Slf4j
public class AggregatorKafkaTopicConfig {

    private List<String> consumerSubscription;
    private String producerTopic;

    @Bean
    public String producerTopic() {
        log.debug("Configured producer topic: {}", producerTopic);
        return producerTopic;
    }

    @Bean
    public List<String> consumerTopic() {
        log.debug("Configured consumer topics: {}", consumerSubscription);
        return consumerSubscription;
    }

    @PostConstruct
    private void logInjectedValues() {
        log.info("Consumer topics: {}", consumerSubscription);
        log.info("Producer topic: {}", producerTopic);
    }
}
