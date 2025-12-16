package ru.yandex.practicum.service;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.KafkaConsumerService;
import ru.yandex.practicum.kafka.KafkaProducerService;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

import java.time.Duration;
import java.util.List;

/**
 * AggregationStarter is the main component responsible for starting and managing
 * the sensor data aggregation process. It consumes sensor events from Kafka,
 * processes them using SensorSnapshotService, and produces snapshots back to Kafka.
 *
 * This component runs an infinite loop that polls for sensor events, updates
 * the current state of sensors, and generates snapshots when needed. It handles
 * graceful shutdown through JVM shutdown hooks and proper resource cleanup.
 */
@Slf4j
@Component
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
public class AggregationStarter {

    final KafkaConsumerService consumer;
    final KafkaProducerService producer;
    final SensorSnapshotService sensorSnapshotService;

    /**
     * Name of the Kafka topic to consume sensor events from
     */
    @Value("${kafka.sensor-topic}")
    String sensorTopic;

    /**
     * Name of the Kafka topic to produce sensor snapshots to
     */
    @Value("${kafka.snapshot-topic}")
    String snapshotTopic;

    /**
     * Starts the aggregation process by subscribing to the sensor topic and
     * continuously polling for messages. For each received sensor event, it
     * updates the sensor state and potentially produces a snapshot.
     *
     * The method handles:
     * - Graceful shutdown via shutdown hook that triggers consumer wakeup
     * - Message consumption and processing in batches
     * - State updates using SensorSnapshotService
     * - Snapshot production to Kafka
     * - Synchronous offset commits
     * - Exception handling and resource cleanup
     */
    public void start() {
        try {
            Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));
            consumer.subscribe(List.of(sensorTopic));

            while (true) {
                ConsumerRecords<String, SpecificRecordBase> records = consumer.poll(Duration.ofMillis(5000));

                if (!records.isEmpty()) {
                    for (ConsumerRecord<String, SpecificRecordBase> record : records) {
                        SensorEventAvro event = (SensorEventAvro) record.value();
                        sensorSnapshotService.updateState(event).ifPresent(snapshot ->
                                producer.send(snapshotTopic, snapshot.getHubId(), snapshot));
                    }
                    consumer.commitSync();
                }
            }
        } catch (WakeupException ignored) {
            log.error("WakeupException received");
        } catch (Exception e) {
            log.error("Error occurred while processing sensor messages", e);
        } finally {
            try {
                producer.flush();
                consumer.commitSync();
            } catch (Exception e) {
                log.error("Error occurred while flushing data", e);
            } finally {
                consumer.close();
                producer.close();
            }
        }
    }
}
