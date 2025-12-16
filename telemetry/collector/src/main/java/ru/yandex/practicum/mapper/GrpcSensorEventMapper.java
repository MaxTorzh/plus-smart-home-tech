package ru.yandex.practicum.mapper;


import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.ClimateSensorEventProto;
import ru.yandex.practicum.grpc.telemetry.event.LightSensorEventProto;
import ru.yandex.practicum.grpc.telemetry.event.MotionSensorEventProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.grpc.telemetry.event.SwitchSensorEventProto;
import ru.yandex.practicum.grpc.telemetry.event.TemperatureSensorEventProto;
import ru.yandex.practicum.kafka.telemetry.event.ClimateSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.LightSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.MotionSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SwitchSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.TemperatureSensorAvro;

import java.time.Instant;

/**
 * GrpcSensorEventMapper is a utility component that converts gRPC SensorEventProto messages
 * to Avro SensorEventAvro objects for Kafka serialization.
 *
 * This mapper handles the conversion between Protocol Buffer representations used
 * in gRPC communication and Avro representations used for Kafka messaging for various
 * types of sensor events.
 */
@Component
public class GrpcSensorEventMapper {

    /**
     * Converts a SensorEventProto gRPC message to a SensorEventAvro Avro object.
     *
     * @param event the gRPC sensor event to convert
     * @return the converted Avro sensor event
     * @throws IllegalArgumentException if the event payload type is not supported
     */
    public SensorEventAvro toAvro(SensorEventProto event) {
        Object payload = switch (event.getPayloadCase()) {
            case LIGHT_SENSOR_EVENT -> toLightSensorAvro(event.getLightSensorEvent());
            case MOTION_SENSOR_EVENT -> toMotionSensorAvro(event.getMotionSensorEvent());
            case CLIMATE_SENSOR_EVENT -> toClimateSensorAvro(event.getClimateSensorEvent());
            case SWITCH_SENSOR_EVENT -> toSwitchSensorAvro(event.getSwitchSensorEvent());
            case TEMPERATURE_SENSOR_EVENT -> toTemperatureSensorAvro(event.getTemperatureSensorEvent());
            default -> throw new IllegalArgumentException("Event type not found " + event.getClass().getName());
        };
        Instant timestamp = Instant.ofEpochSecond(event.getTimestamp().getSeconds(), event.getTimestamp().getNanos());

        return SensorEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setId(event.getId())
                .setTimestamp(timestamp)
                .setPayload(payload)
                .build();
    }

    /**
     * Converts a ClimateSensorEventProto to ClimateSensorAvro.
     *
     * @param event the gRPC climate sensor event to convert
     * @return the converted Avro climate sensor event
     */
    private ClimateSensorAvro toClimateSensorAvro(ClimateSensorEventProto event) {
        return ClimateSensorAvro.newBuilder()
                .setTemperatureC(event.getTemperatureC())
                .setHumidity(event.getHumidity())
                .setCo2Level(event.getCo2Level())
                .build();
    }

    /**
     * Converts a LightSensorEventProto to LightSensorAvro.
     *
     * @param event the gRPC light sensor event to convert
     * @return the converted Avro light sensor event
     */
    private LightSensorAvro toLightSensorAvro(LightSensorEventProto event) {
        return LightSensorAvro.newBuilder()
                .setLinkQuality(event.getLinkQuality())
                .setLuminosity(event.getLuminosity())
                .build();
    }

    /**
     * Converts a MotionSensorEventProto to MotionSensorAvro.
     *
     * @param event the gRPC motion sensor event to convert
     * @return the converted Avro motion sensor event
     */
    private MotionSensorAvro toMotionSensorAvro(MotionSensorEventProto event) {
        return MotionSensorAvro.newBuilder()
                .setLinkQuality(event.getLinkQuality())
                .setMotion(event.getMotion())
                .setVoltage(event.getVoltage())
                .build();
    }

    /**
     * Converts a SwitchSensorEventProto to SwitchSensorAvro.
     *
     * @param event the gRPC switch sensor event to convert
     * @return the converted Avro switch sensor event
     */
    private SwitchSensorAvro toSwitchSensorAvro(SwitchSensorEventProto event) {
        return SwitchSensorAvro.newBuilder()
                .setState(event.getState())
                .build();
    }

    /**
     * Converts a TemperatureSensorEventProto to TemperatureSensorAvro.
     *
     * @param event the gRPC temperature sensor event to convert
     * @return the converted Avro temperature sensor event
     */
    private TemperatureSensorAvro toTemperatureSensorAvro(TemperatureSensorEventProto event) {
        return TemperatureSensorAvro.newBuilder()
                .setTemperatureC(event.getTemperatureC())
                .setTemperatureF(event.getTemperatureF())
                .build();
    }
}