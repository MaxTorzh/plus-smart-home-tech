package ru.yandex.practicum.service.handler.sensor;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.configuration.CollectorKafkaConfig;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.grpc.telemetry.event.SwitchSensorProto;
import ru.yandex.practicum.kafka.telemetry.event.SwitchSensorAvro;
import ru.yandex.practicum.producer.KafkaEventProducer;

/**
 * Handler for event from Switch Sensor
 */
@Component
@Slf4j
public class SwitchSensorEventHandler extends BaseSensorEventHandler<SwitchSensorAvro> {

    public SwitchSensorEventHandler(final KafkaEventProducer producer,
                                    final CollectorKafkaConfig config) {
        super(producer, config);
        log.info("SwitchSensorEventHandler instantiated.");
    }

    @Override
    public SensorEventProto.PayloadCase getMessageType() {
        return SensorEventProto.PayloadCase.SWITCH_SENSOR_EVENT;
    }

    @Override
    protected SwitchSensorAvro mapToAvro(final SensorEventProto event) {
        log.debug("Mapping SwitchSensorEvent to Avro: {}", event);
        if (event.getPayloadCase() != SensorEventProto.PayloadCase.SWITCH_SENSOR_EVENT) {
            throw new IllegalArgumentException(
                    "Invalid payload type for SwitchSensorEventHandler: " + event.getPayloadCase());
        }
        final SwitchSensorProto _event = event.getSwitchSensorEvent();

        return SwitchSensorAvro.newBuilder()
                .setState(_event.getState())
                .build();
    }
}
