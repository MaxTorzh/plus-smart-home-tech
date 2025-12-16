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
import ru.yandex.practicum.handler.snapshot.SnapshotHandler;
import ru.yandex.practicum.kafka.ConsumerSnapshotService;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.time.Duration;
import java.util.List;

/**
 * SnapshotProcessor is responsible for consuming and processing sensor snapshots from Kafka.
 * It runs as a separate thread and evaluates scenarios based on current sensor states.
 * When scenario conditions are met, it triggers the appropriate actions.
 *
 * This processor subscribes to the sensor snapshots topic and continuously polls for new snapshots.
 * Each snapshot is processed by the SnapshotHandler to determine if any scenarios should be activated.
 */
@Slf4j
@Component
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
public class SnapshotProcessor {
    final ConsumerSnapshotService consumer;
    final SnapshotHandler snapshotHandler;

    @Value("${kafka.topics.snapshot}")
    String topic;

    /**
     * Starts the snapshot processing loop.
     * Continuously polls for sensor snapshots and processes them.
     */
    public void start() {
        try {
            Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));
            consumer.subscribe(List.of(topic));

            while (true) {
                Thread.sleep(2000);
                ConsumerRecords<String, SpecificRecordBase> records = consumer.poll(Duration.ofMillis(1000));

                if (!records.isEmpty()) {
                    for (ConsumerRecord<String, SpecificRecordBase> record : records) {
                        SensorsSnapshotAvro sensorsSnapshot = (SensorsSnapshotAvro) record.value();
                        snapshotHandler.handleSnapshot(sensorsSnapshot);
                    }
                    consumer.commitSync();
                }
            }

        } catch (WakeupException ignored) {
            log.error("WakeupException received");
        } catch (Exception e) {
            log.error("Error occurred while processing messages", e);
        } finally {
            try {
                consumer.commitSync();
            } catch (Exception e) {
                log.error("Error occurred while flushing data", e);
            } finally {
                consumer.close();
            }
        }
    }
}