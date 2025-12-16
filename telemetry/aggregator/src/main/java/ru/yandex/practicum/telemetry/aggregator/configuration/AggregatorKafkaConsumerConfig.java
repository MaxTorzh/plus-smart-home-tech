package ru.yandex.practicum.telemetry.aggregator.configuration;

import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

import java.util.Properties;

@Getter
@Setter
@ConfigurationProperties(prefix = "aggregator.kafka.consumer")
@Configuration
@Slf4j
public class AggregatorKafkaConsumerConfig {

    private String bootstrapServers;
    private String groupId;
    private String autoOffsetReset;
    private boolean enableAutoCommit;
    private String keyDeserializer;
    private String valueDeserializer;

    @Bean
    public KafkaConsumer<String, SensorEventAvro> kafkaConsumer() {
        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);

        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);

        return new KafkaConsumer<>(config);
    }

    @PostConstruct
    private void validateConfig() {
        if (bootstrapServers == null || groupId == null ||
                autoOffsetReset == null || keyDeserializer == null ||
                valueDeserializer == null) {
            log.error("Invalid Kafka consumer configuration.");
            throw new IllegalStateException("Missing required Kafka configuration.");
        }
        log.debug("Kafka consumer configuration validated successfully.");
    }
}
