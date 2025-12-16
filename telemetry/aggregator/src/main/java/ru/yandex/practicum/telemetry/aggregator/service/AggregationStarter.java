package ru.yandex.practicum.telemetry.aggregator.service;

import java.time.Duration;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.telemetry.aggregator.configuration.kafka.AggregatorKafkaTopicConfig;

/**
 * {@code AggregationStarter} is responsible for initiating and managing the data aggregation
 * process by consuming events from Kafka topics, processing sensor reading data, and producing
 * snapshots of the sensors state associated with a certain hub back to Kafka.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class AggregationStarter {

    private final KafkaProducer<String, SensorsSnapshotAvro> producer;
    private final KafkaConsumer<String, SensorEventAvro> consumer;
    private final SnapshotService snapshotService;
    private final AggregatorKafkaTopicConfig topics;

    public void start() {
        log.debug("Starting aggregation process.");
        try {
            Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));

            consumer.subscribe(topics.getConsumerSubscription());
            log.info("Subscribed for the topic: {}", topics.getConsumerSubscription());

            while (true) {
                ConsumerRecords<String, SensorEventAvro> records = consumer.poll(Duration.ofMillis(1000));
                if (!records.isEmpty()) {

                    for (ConsumerRecord<String, SensorEventAvro> record : records) {
                        processRecord(record);
                    }
                    consumer.commitSync();
                }
            }
        } catch (WakeupException ignored) {
            log.warn("WakeupException caught - Consumer shutting down.");
        } catch (Exception e) {
            log.error("Error during event processing", e);
        } finally {
            cleanupResources();
        }

    }

    /**
     * Processes a single consumer record and generates a snapshot.
     *
     * @param record the Kafka consumer record
     */
    private void processRecord(final ConsumerRecord<String, SensorEventAvro> record) {
        log.info(
                "Processing  ConsumerRecord: topic={}, partition={}, offset={}, hubId={}, timestamp={}",
                record.topic(), record.partition(), record.offset(), record.key(), record.timestamp());

        final Optional<SensorsSnapshotAvro> updatedSnapshot = snapshotService.updateState(
                record.value());
        updatedSnapshot.ifPresent(this::sendSnapshotToKafka);
    }

    /**
     * Sends a generated snapshot to Kafka.
     *
     * @param snapshot the snapshot to be sent
     */
    private void sendSnapshotToKafka(final SensorsSnapshotAvro snapshot) {
        log.info("Sending snapshot to Kafka: hubId={}", snapshot.getHubId());

        final ProducerRecord<String, SensorsSnapshotAvro> record =
                new ProducerRecord<>(topics.getProducerTopic(), snapshot.getHubId(), snapshot);
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                log.error("Failed to send snapshot to Kafka", exception);
            } else {
                log.info("Snapshot sent successfully to the topic {} at offset {}", metadata.topic(),
                        metadata.offset());
            }
        });
    }

    /**
     * Cleans up Kafka producer and consumer resources.
     */
    private void cleanupResources() {
        try {
            log.info("Flushing producer buffer.");
            producer.flush();

            log.info("Committing consumer offsets synchronously.");
            consumer.commitSync();
        } catch (Exception e) {
            log.error("Error during resource cleanup", e);
        } finally {
            log.info("Closing Kafka consumer.");
            consumer.close();
            log.info("Closing Kafka producer.");
            producer.close();
        }
    }
}
