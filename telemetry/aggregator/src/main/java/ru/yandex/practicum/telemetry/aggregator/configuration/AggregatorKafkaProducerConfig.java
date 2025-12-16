package ru.yandex.practicum.telemetry.aggregator.configuration;

import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.util.Properties;

@Getter
@Setter
@ConfigurationProperties(prefix = "aggregator.kafka.producer")
@Configuration
@Slf4j
public class AggregatorKafkaProducerConfig {

    private String bootstrapServers;
    private String keySerializer;
    private String valueSerializer;

    @Bean
    public KafkaProducer<String, SensorsSnapshotAvro> kafkaProducer() {
        log.debug("Initializing Kafka producer with bootstrap servers: {}", bootstrapServers);
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);
        return new KafkaProducer<>(config);
    }

    @PostConstruct
    private void validateConfig() {
        if (bootstrapServers == null || keySerializer == null || valueSerializer == null) {
            log.error("Invalid Kafka producer configuration.");
            throw new IllegalStateException("Missing required Kafka configuration.");
        }
        log.debug("Kafka producer configuration validated successfully.");
    }
}
