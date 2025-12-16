package ru.yandex.practicum.telemetry.collector.configuration;

import jakarta.annotation.PostConstruct;
import java.util.Map;
import java.util.Properties;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration class that manages Kafka settings from the application properties, including bootstrap servers, topics, and producer settings.
 * <p>
 * This class is responsible for configuring the Kafka producer and retrieving topic names based on topic enums.
 */
@Getter
@Setter
@ConfigurationProperties(prefix = "collector.kafka")
@Configuration
@Slf4j
public class CollectorKafkaConfig {

    private String bootstrapServers;
    private Map<String, String> topics;
    private Map<String, String> producer;

    @Bean
    public Producer<String, SpecificRecordBase> kafkaProducer() {
        log.debug("Initializing Kafka producer with bootstrap servers: {}", bootstrapServers);
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, producer.get("key-serializer"));
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, producer.get("value-serializer"));
        return new KafkaProducer<>(config);
    }

    public String getTopic(String topicEnum) {
        String topicName = topics.getOrDefault(topicEnum, null);
        if (topicName == null) {
            log.error("Topic {} not found in the configuration.", topicEnum);
            throw new IllegalArgumentException("Undefined Kafka topic: " + topicEnum);
        }
        return topicName;
    }

    @PostConstruct
    private void validateConfig() {
        if (bootstrapServers == null || topics.isEmpty() || producer.isEmpty()) {
            log.error("Invalid Kafka configuration. Missing bootstrapServers, topics, or producer settings.");
            throw new IllegalStateException("Missing required Kafka configuration.");
        }
        log.debug("Kafka configuration validated successfully.");
    }
}
