package ru.yandex.practicum.telemetry.collector.service.handler.sensor;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.telemetry.collector.configuration.CollectorKafkaConfig;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.grpc.telemetry.event.TemperatureSensorProto;
import ru.yandex.practicum.kafka.telemetry.event.TemperatureSensorAvro;
import ru.yandex.practicum.telemetry.collector.producer.KafkaEventProducer;

/**
 * Handler for event from Temperature Sensor
 */
@Component
@Slf4j
public class TemperatureSensorEventHandler extends BaseSensorEventHandler<TemperatureSensorAvro> {

    public TemperatureSensorEventHandler(final KafkaEventProducer producer,
                                         final CollectorKafkaConfig config) {
        super(producer, config);
        log.info("TemperatureSensorEventHandler instantiated.");
    }

    @Override
    public SensorEventProto.PayloadCase getMessageType() {
        return SensorEventProto.PayloadCase.TEMPERATURE_SENSOR_EVENT;
    }

    @Override
    protected TemperatureSensorAvro mapToAvro(final SensorEventProto event) {
        log.debug("Mapping TemperatureSensorEvent to Avro: {}", event);
        if (event.getPayloadCase() != SensorEventProto.PayloadCase.TEMPERATURE_SENSOR_EVENT) {
            throw new IllegalArgumentException(
                    "Invalid payload type for TemperatureSensorEventHandler: " + event.getPayloadCase());
        }
        final TemperatureSensorProto _event = event.getTemperatureSensorEvent();

        return TemperatureSensorAvro.newBuilder()
                .setTemperatureC(_event.getTemperatureC())
                .setTemperatureF(_event.getTemperatureC())
                .build();
    }
}
