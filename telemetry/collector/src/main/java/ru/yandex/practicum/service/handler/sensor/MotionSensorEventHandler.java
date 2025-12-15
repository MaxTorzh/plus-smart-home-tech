package ru.yandex.practicum.service.handler.sensor;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.configuration.CollectorKafkaConfig;
import ru.yandex.practicum.grpc.telemetry.event.MotionSensorProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.kafka.telemetry.event.MotionSensorAvro;
import ru.yandex.practicum.producer.KafkaEventProducer;

/**
 * Handler for event from Motion Sensor
 */
@Component
@Slf4j
public class MotionSensorEventHandler extends BaseSensorEventHandler<MotionSensorAvro> {

    public MotionSensorEventHandler(final KafkaEventProducer producer,
                                    final CollectorKafkaConfig config) {
        super(producer, config);
        log.info("MotionSensorEventHandler instantiated.");
    }

    @Override
    public SensorEventProto.PayloadCase getMessageType() {
        return SensorEventProto.PayloadCase.MOTION_SENSOR_EVENT;
    }

    @Override
    protected MotionSensorAvro mapToAvro(final SensorEventProto event) {
        log.debug("Mapping MotionSensorEvent to Avro: {}", event);
        if (event.getPayloadCase() != SensorEventProto.PayloadCase.MOTION_SENSOR_EVENT) {
            throw new IllegalArgumentException(
                    "Invalid payload type for MotionSensorEventHandler: " + event.getPayloadCase());
        }
        final MotionSensorProto _event = event.getMotionSensorEvent();

        return MotionSensorAvro.newBuilder()
                .setLinkQuality(_event.getLinkQuality())
                .setMotion(_event.getMotion())
                .setVoltage(_event.getVoltage())
                .build();
    }
}
