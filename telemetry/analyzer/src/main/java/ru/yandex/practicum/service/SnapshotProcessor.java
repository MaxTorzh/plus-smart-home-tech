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

@Slf4j
@Component
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
public class SnapshotProcessor {
    final ConsumerSnapshotService consumer;
    final SnapshotHandler snapshotHandler;

    @Value("${kafka.topics.snapshot}")
    String topic;

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
            log.error("Получено исключение WakeupException");
        } catch (Exception e) {
            log.error("Возникла ошибка при обработке сообщений", e);
        } finally {
            try {
                consumer.commitSync();
            } catch (Exception e) {
                log.error("Возникла ошибка при сбросе данных", e);
            } finally {
                consumer.close();
            }
        }
    }
}