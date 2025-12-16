package ru.yandex.practicum.kafka;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import serializer.GeneralAvroSerializer;

import java.time.Duration;
import java.util.Properties;

/**
 * KafkaProducerService is a service class that provides Kafka producer functionality
 * for sending sensor events to Kafka topics.
 *
 * This service wraps the KafkaProducer and provides simplified methods for sending
 * messages with Avro serialization.
 *
 * The producer is configured with:
 * - String key serializer
 * - Custom GeneralAvroSerializer for value serialization
 * - Configurable bootstrap servers via application properties
 */
@Service
public class KafkaProducerService implements AutoCloseable {

    private final KafkaProducer<String, SpecificRecordBase> producer;

    /**
     * Constructs a new KafkaProducerService with the specified bootstrap servers.
     *
     * @param bootstrapServers comma-separated list of Kafka broker addresses
     */
    public KafkaProducerService(@Value("${kafka.bootstrap-servers}") String bootstrapServers) {
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GeneralAvroSerializer.class.getName());

        this.producer = new KafkaProducer<>(config);
    }

    /**
     * Sends a message to the specified Kafka topic with the given key and value.
     * This method blocks until the message is sent and then flushes the producer.
     *
     * @param topic the name of the topic to send the message to
     * @param key the key of the message
     * @param value the Avro record value of the message
     */
    public void send(String topic, String key, SpecificRecordBase value){
        producer.send(new ProducerRecord<>(topic, key, value));
        producer.flush();
    }

    /**
     * Flushes any accumulated messages in the producer's buffer.
     */
    public void flush() {
        producer.flush();
    }

    /**
     * Flushes and closes the producer, waiting up to 5 seconds for completion.
     * This method should be called before shutting down the application.
     */
    @Override
    public void close() {
        producer.flush();
        producer.close(Duration.ofSeconds(5));
    }
}