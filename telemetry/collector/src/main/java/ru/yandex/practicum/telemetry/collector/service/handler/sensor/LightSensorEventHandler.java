package ru.yandex.practicum.telemetry.collector.service.handler.sensor;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.telemetry.collector.configuration.CollectorKafkaConfig;
import ru.yandex.practicum.grpc.telemetry.event.LightSensorProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.kafka.telemetry.event.LightSensorAvro;
import ru.yandex.practicum.telemetry.collector.producer.KafkaEventProducer;

/**
 * Handler for event from Light Sensor
 */
@Component
@Slf4j
public class LightSensorEventHandler extends BaseSensorEventHandler<LightSensorAvro> {

    public LightSensorEventHandler(final KafkaEventProducer producer,
                                   final CollectorKafkaConfig config) {
        super(producer, config);
        log.info("LightSensorEventHandler instantiated.");
    }

    @Override
    public SensorEventProto.PayloadCase getMessageType() {
        return SensorEventProto.PayloadCase.LIGHT_SENSOR_EVENT;
    }

    @Override
    protected LightSensorAvro mapToAvro(final SensorEventProto event) {
        log.debug("Mapping LightSensorEvent to Avro: {}", event);

        if (event.getPayloadCase() != SensorEventProto.PayloadCase.LIGHT_SENSOR_EVENT) {
            throw new IllegalArgumentException(
                    "Invalid payload type for LightSensorEvent: " + event.getPayloadCase());
        }
        final LightSensorProto _event = event.getLightSensorEvent();

        return LightSensorAvro.newBuilder()
                .setLinkQuality(_event.getLinkQuality())
                .setLuminosity(_event.getLuminosity())
                .build();
    }
}
