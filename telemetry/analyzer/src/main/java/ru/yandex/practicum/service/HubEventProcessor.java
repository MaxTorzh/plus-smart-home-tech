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
import ru.yandex.practicum.handler.hub.HubEventHandler;
import ru.yandex.practicum.handler.hub.HubEventHandlers;
import ru.yandex.practicum.kafka.ConsumerHubService;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;

import java.time.Duration;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
public class HubEventProcessor implements Runnable {
    final ConsumerHubService consumer;
    final HubEventHandlers hubHandlers;

    @Value("${kafka.topics.hub}")
    String topic;

    @Override
    public void run() {
        try {
            Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));
            consumer.subscribe(List.of(topic));
            Map<String, HubEventHandler> hubHandlersMap = hubHandlers.getHandlers();

            while (true) {
                ConsumerRecords<String, SpecificRecordBase> records = consumer.poll(Duration.ofMillis(1000));
                if (!records.isEmpty()) {
                    for (ConsumerRecord<String, SpecificRecordBase> record : records) {
                        HubEventAvro hubEvent = (HubEventAvro) record.value();
                        String payloadName = hubEvent.getPayload().getClass().getSimpleName();

                        if (hubHandlersMap.containsKey(payloadName)) {
                            hubHandlersMap.get(payloadName).handle(hubEvent);
                        } else {
                            throw new IllegalArgumentException("Не найден обработчик для события: " + hubEvent);
                        }
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
