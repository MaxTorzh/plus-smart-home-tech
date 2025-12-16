package ru.yandex.practicum.telemetry.collector.service.handler.sensor;

import java.time.Instant;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.beans.factory.annotation.Autowired;
import ru.yandex.practicum.telemetry.collector.configuration.CollectorKafkaConfig;
import ru.yandex.practicum.telemetry.collector.configuration.KafkaTopic;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.telemetry.collector.producer.KafkaEventProducer;

/**
 * Base class for handling sensor events.
 *
 * @param <T> The type of Avro record to map the sensor event to.
 */
@Slf4j
public abstract class BaseSensorEventHandler<T extends SpecificRecordBase> implements
        SensorEventHandler {

    protected final KafkaEventProducer producer;
    protected final String topic;

    @Autowired
    protected BaseSensorEventHandler(final KafkaEventProducer producer,
                                     final CollectorKafkaConfig config) {
        this.producer = producer;
        this.topic = KafkaTopic.SENSORS.getTopicName(config);
        log.debug("Initialized BaseSensorEventHandler with topic '{}'.", topic);
    }

    protected abstract T mapToAvro(SensorEventProto event);

    @Override
    public void handle(final SensorEventProto event) {
        Objects.requireNonNull(event);
        log.debug("Starting handling event {}", event);

        validateEventType(event);

        T payload = mapToAvro(event);
        log.info("Mapped event to payload: {}", payload.getClass().getName());

        final SensorEventAvro eventAvro = buildEventAvro(event, payload);

        producer.send(eventAvro, event.getHubId(), eventAvro.getTimestamp(), topic);
        log.info("Sensor event successfully sent to the topic '{}'.", topic);
    }

    private SensorEventAvro buildEventAvro(SensorEventProto event, T payload) {
        log.debug("Building Avro event for: {}", payload.getClass().getName());

        final Instant timestamp = Instant.ofEpochSecond(
                        event.getTimestamp().getSeconds(),
                        event.getTimestamp().getNanos())
                .truncatedTo(java.time.temporal.ChronoUnit.MILLIS);

        return SensorEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setId(event.getId())
                .setTimestamp(timestamp)
                .setPayload(payload)
                .build();
    }

    private void validateEventType(final SensorEventProto event) {
        if (!event.getPayloadCase().equals(getMessageType())) {
            log.warn("Wrong event type: {}.", event.getPayloadCase());
            throw new IllegalArgumentException("Unknown event type: " + event.getPayloadCase());
        }
    }
}
