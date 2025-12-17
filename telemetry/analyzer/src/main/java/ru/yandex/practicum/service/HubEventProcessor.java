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

/**
 * HubEventProcessor is responsible for consuming and processing hub events from Kafka.
 * It runs as a separate thread and dispatches events to appropriate handlers based on event type.
 *
 * This processor subscribes to the hub events topic and continuously polls for new events.
 * When events arrive, it identifies the appropriate handler and delegates processing to it.
 */
@Slf4j
@Component
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
public class HubEventProcessor implements Runnable {
    final ConsumerHubService consumer;
    final HubEventHandlers hubHandlers;

    /**
     * Kafka topic name for hub events
     */
    @Value("${kafka.topics.hub}")
    String topic;

    /**
     * Main execution method that runs the hub event processing loop.
     * Continuously polls for hub events and processes them using appropriate handlers.
     */
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
