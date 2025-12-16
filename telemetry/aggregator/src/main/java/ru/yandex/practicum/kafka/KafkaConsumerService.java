package ru.yandex.practicum.kafka;

import deserializer.SensorEventDeserializer;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * KafkaConsumerService is a service class that provides Kafka consumer functionality
 * for consuming sensor events from Kafka topics.
 *
 * This service wraps the KafkaConsumer and provides simplified methods for polling,
 * subscribing to topics, and managing consumer lifecycle operations.
 *
 * The consumer is configured with:
 * - String key deserializer
 * - Custom SensorEventDeserializer for value deserialization
 * - Configurable group ID, auto-commit and bootstrap servers via application properties
 */
@Service
public class KafkaConsumerService implements AutoCloseable {

    private final KafkaConsumer<String, SpecificRecordBase> consumer;

    /**
     * Constructs a new KafkaConsumerService with the specified configuration.
     *
     * @param groupId the consumer group ID
     * @param autoCommit the auto commit configuration ("true" or "false")
     * @param bootstrapServers comma-separated list of Kafka broker addresses
     */
    public KafkaConsumerService(
            @Value("${kafka.group-id}") String groupId,
            @Value("${kafka.auto-commit}") String autoCommit,
            @Value("${kafka.bootstrap-servers}") String bootstrapServers) {
        Properties config = new Properties();
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, autoCommit);
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SensorEventDeserializer.class.getName());

        this.consumer = new KafkaConsumer<>(config);
    }

    /**
     * Polls for records from subscribed topics for the specified duration.
     *
     * @param duration the maximum time to block waiting for records
     * @return the records fetched from Kafka
     */
    public ConsumerRecords<String, SpecificRecordBase> poll(Duration duration){
        return consumer.poll(duration);
    }

    /**
     * Subscribes the consumer to the given list of topics.
     *
     * @param topics list of topic names to subscribe to
     */
    public void subscribe(List<String> topics){
        consumer.subscribe(topics);
    }

    /**
     * Commits all consumed offsets synchronously.
     */
    public void commitSync(){
        consumer.commitSync();
    }

    /**
     * Wakes up the consumer by throwing WakeupException in the thread blocked on poll().
     * Used to interrupt consumer polling from another thread.
     */
    public void wakeup(){
        consumer.wakeup();
    }

    /**
     * Closes the consumer, waiting if necessary for any pending requests to complete.
     * This method should be called before shutting down the application.
     */
    @Override
    public void close() {
        consumer.close();
    }
}
