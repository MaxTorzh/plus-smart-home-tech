package ru.yandex.practicum.configuration;

import lombok.Getter;

/**
 * Enum representing Kafka topics used in the system. Each enum corresponds to a specific topic and
 * provides a way to retrieve the topic name from the application's configuration based on the
 * enum's key.
 */
@Getter
public enum KafkaTopic {
    SENSORS("sensors"),
    HUBS("hubs");

    private final String topicKey;

    KafkaTopic(String topicKey) {
        this.topicKey = topicKey;
    }

    /**
     * Get the default topic name associated with the enum.
     *
     * @param config The CollectorKafkaConfig instance to access topic configuration.
     * @return The corresponding topic name from the config.
     */
    public String getTopicName(CollectorKafkaConfig config) {
        return config.getTopic(this.topicKey);
    }
}
