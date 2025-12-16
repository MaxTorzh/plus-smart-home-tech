package ru.yandex.practicum.telemetry.collector.service.handler.sensor;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.telemetry.collector.configuration.CollectorKafkaConfig;
import ru.yandex.practicum.grpc.telemetry.event.ClimateSensorProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.kafka.telemetry.event.ClimateSensorAvro;
import ru.yandex.practicum.telemetry.collector.producer.KafkaEventProducer;

/**
 * Handler for event from Climate Sensor
 */
@Component
@Slf4j
public class ClimateSensorEventHandler extends BaseSensorEventHandler<ClimateSensorAvro> {

    public ClimateSensorEventHandler(KafkaEventProducer producer, CollectorKafkaConfig config) {
        super(producer, config);
        log.info("ClimateSensorEventHandler instantiated.");
    }

    @Override
    public SensorEventProto.PayloadCase getMessageType() {
        return SensorEventProto.PayloadCase.CLIMATE_SENSOR_EVENT;
    }

    @Override
    protected ClimateSensorAvro mapToAvro(final SensorEventProto event) {
        log.debug("Mapping ClimateSensorEvent to Avro: {}", event);
        if (event.getPayloadCase() != SensorEventProto.PayloadCase.CLIMATE_SENSOR_EVENT) {
            throw new IllegalArgumentException(
                    "Invalid payload type for ClimateSensorEventHandler: " + event.getPayloadCase());
        }
        final ClimateSensorProto _event = event.getClimateSensorEvent();

        return ClimateSensorAvro.newBuilder()
                .setCo2Level(_event.getCo2Level())
                .setHumidity(_event.getHumidity())
                .setTemperatureC(_event.getTemperatureC())
                .build();
    }
}
