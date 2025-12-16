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

@Component
public class GrpcSensorEventMapper {
    public SensorEventAvro toAvro(SensorEventProto event) {
        Object payload = switch (event.getPayloadCase()) {
            case LIGHT_SENSOR_EVENT -> toLightSensorAvro(event.getLightSensorEvent());
            case MOTION_SENSOR_EVENT -> toMotionSensorAvro(event.getMotionSensorEvent());
            case CLIMATE_SENSOR_EVENT -> toClimateSensorAvro(event.getClimateSensorEvent());
            case SWITCH_SENSOR_EVENT -> toSwitchSensorAvro(event.getSwitchSensorEvent());
            case TEMPERATURE_SENSOR_EVENT -> toTemperatureSensorAvro(event.getTemperatureSensorEvent());
            default -> throw new IllegalArgumentException("Не найден тип события " + event.getClass().getName());
        };
        Instant timestamp = Instant.ofEpochSecond(event.getTimestamp().getSeconds(), event.getTimestamp().getNanos());

        return SensorEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setId(event.getId())
                .setTimestamp(timestamp)
                .setPayload(payload)
                .build();
    }

    private ClimateSensorAvro toClimateSensorAvro(ClimateSensorEventProto event) {
        return ClimateSensorAvro.newBuilder()
                .setTemperatureC(event.getTemperatureC())
                .setHumidity(event.getHumidity())
                .setCo2Level(event.getCo2Level())
                .build();
    }

    private LightSensorAvro toLightSensorAvro(LightSensorEventProto event) {
        return LightSensorAvro.newBuilder()
                .setLinkQuality(event.getLinkQuality())
                .setLuminosity(event.getLuminosity())
                .build();
    }

    private MotionSensorAvro toMotionSensorAvro(MotionSensorEventProto event) {
        return MotionSensorAvro.newBuilder()
                .setLinkQuality(event.getLinkQuality())
                .setMotion(event.getMotion())
                .setVoltage(event.getVoltage())
                .build();
    }

    private SwitchSensorAvro toSwitchSensorAvro(SwitchSensorEventProto event) {
        return SwitchSensorAvro.newBuilder()
                .setState(event.getState())
                .build();
    }

    private TemperatureSensorAvro toTemperatureSensorAvro(TemperatureSensorEventProto event) {
        return TemperatureSensorAvro.newBuilder()
                .setTemperatureC(event.getTemperatureC())
                .setTemperatureF(event.getTemperatureF())
                .build();
    }
}