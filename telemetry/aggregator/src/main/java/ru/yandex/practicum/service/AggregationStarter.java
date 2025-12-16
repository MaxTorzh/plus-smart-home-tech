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

@Slf4j
@Component
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
public class AggregationStarter {

    final KafkaConsumerService consumer;
    final KafkaProducerService producer;
    final SensorSnapshotService sensorSnapshotService;

    @Value("${kafka.sensor-topic}")
    String sensorTopic;

    @Value("${kafka.snapshot-topic}")
    String snapshotTopic;

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
            log.error("Получено исключение WakeupException");
        } catch (Exception e) {
            log.error("Возникла ошибка при обработке сообщений от датчиков", e);
        } finally {
            try {
                producer.flush();
                consumer.commitSync();
            } catch (Exception e) {
                log.error("Возникла ошибка при сбросе данных", e);
            } finally {
                consumer.close();
                producer.close();
            }
        }
    }
}
