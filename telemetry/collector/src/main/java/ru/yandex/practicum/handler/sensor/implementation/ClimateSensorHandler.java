package ru.yandex.practicum.handler.sensor.implementation;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.handler.sensor.SensorEventHandler;
import ru.yandex.practicum.kafka.KafkaProducerService;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.mapper.GrpcSensorEventMapper;

import java.time.Instant;

@Component
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
public class ClimateSensorHandler implements SensorEventHandler {
    final KafkaProducerService kafkaProducer;
    final GrpcSensorEventMapper sensorEventMapper;

    @Value("${kafka.sensor-topic}")
    String topic;

    @Override
    public SensorEventProto.PayloadCase getMessageType() {
        return SensorEventProto.PayloadCase.CLIMATE_SENSOR_EVENT;
    }

    @Override
    public void handle(SensorEventProto event) {
        SensorEventAvro sensorEventAvro = sensorEventMapper.toAvro(event);
        Instant timestamp = Instant.ofEpochSecond(event.getTimestamp().getSeconds(), event.getTimestamp().getNanos());
        kafkaProducer.send(sensorEventAvro, sensorEventAvro.getHubId(), timestamp, topic);
    }
}
