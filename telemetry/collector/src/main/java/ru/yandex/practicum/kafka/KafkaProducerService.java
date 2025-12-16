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
import java.time.Instant;
import java.util.Properties;

/**
 * KafkaProducerService is a service class that provides Kafka producer functionality
 * for sending telemetry events to Kafka topics.
 *
 * This service wraps the KafkaProducer and provides simplified methods for sending
 * Avro serialized events with proper key-value pairing and timestamping.
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
     * Sends an event to the specified Kafka topic with the provided metadata.
     *
     * @param event the Avro record to send
     * @param hubId the hub identifier used as the message key
     * @param timestamp the timestamp for the message
     * @param topic the Kafka topic to send the message to
     */
    public void send(SpecificRecordBase event, String hubId, Instant timestamp, String topic){
        ProducerRecord<String, SpecificRecordBase> record = new ProducerRecord<>(
                topic,
                null,
                timestamp.toEpochMilli(),
                hubId,
                event
        );
        producer.send(record);
        producer.flush();
    }

    /**
     * Closes the producer, flushing any pending messages and waiting up to 5 seconds
     * for the close operation to complete.
     */
    @Override
    public void close() {
        producer.flush();
        producer.close(Duration.ofSeconds(5));
    }
}
