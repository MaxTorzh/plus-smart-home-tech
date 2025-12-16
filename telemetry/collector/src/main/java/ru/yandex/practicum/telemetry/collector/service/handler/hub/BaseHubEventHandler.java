package ru.yandex.practicum.telemetry.collector.service.handler.hub;

import java.time.Instant;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.beans.factory.annotation.Autowired;
import ru.yandex.practicum.telemetry.collector.configuration.CollectorKafkaConfig;
import ru.yandex.practicum.telemetry.collector.configuration.KafkaTopic;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.telemetry.collector.producer.KafkaEventProducer;


/**
 * Base class for handling hub events.
 *
 * @param <T> The type of Avro record to map the hub event to.
 */
@Slf4j
public abstract class BaseHubEventHandler<T extends SpecificRecordBase> implements HubEventHandler {

    protected final KafkaEventProducer producer;
    protected final String topic;

    @Autowired
    protected BaseHubEventHandler(final KafkaEventProducer producer,
                                  final CollectorKafkaConfig config) {
        this.producer = producer;
        this.topic = KafkaTopic.HUBS.getTopicName(config);
        log.debug("Initialized BaseHubEventHandler with topic '{}'.", topic);
    }

    protected abstract T mapToAvro(HubEventProto event);

    @Override
    public void handle(final HubEventProto event) {
        Objects.requireNonNull(event);
        log.debug("Starting handling hub event {}", event);

        validateEventType(event);

        T payload = mapToAvro(event);
        log.info("Mapped hub event to payload: {}", payload.getClass().getName());

        final HubEventAvro eventAvro = buildEventAvro(event, payload);

        producer.send(eventAvro, event.getHubId(), eventAvro.getTimestamp(), topic);
        log.info("Hub event successfully sent to the topic '{}'.", topic);
    }

    private HubEventAvro buildEventAvro(HubEventProto event, T payload) {
        log.debug("Building Avro hub event for: {}", payload.getClass().getName());

        final Instant timestamp = Instant.ofEpochSecond(
                        event.getTimestamp().getSeconds(),
                        event.getTimestamp().getNanos())
                .truncatedTo(java.time.temporal.ChronoUnit.MILLIS);

        return HubEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(timestamp)
                .setPayload(payload)
                .build();
    }

    private void validateEventType(final HubEventProto event) {
        if (!event.getPayloadCase().equals(getMessageType())) {
            log.warn("Wrong hub event type: {}.", event.getPayloadCase());
            throw new IllegalArgumentException("Unknown hub event type: " + event.getPayloadCase());
        }
    }
}
