package ru.yandex.practicum.handler.hub.implementation;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.handler.hub.HubEventHandler;
import ru.yandex.practicum.kafka.KafkaProducerService;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.mapper.GrpcHubEventMapper;

import java.time.Instant;

@Component
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
public class DeviceAddedHandler implements HubEventHandler {
    final KafkaProducerService kafkaProducer;
    final GrpcHubEventMapper hubEventMapper;

    @Value("${kafka.hub-topic}")
    String topic;

    @Override
    public HubEventProto.PayloadCase getMessageType() {
        return HubEventProto.PayloadCase.DEVICE_ADDED;
    }

    @Override
    public void handle(HubEventProto event) {
        HubEventAvro hubEventAvro = hubEventMapper.toAvro(event);
        Instant timestamp = Instant.ofEpochSecond(event.getTimestamp().getSeconds(), event.getTimestamp().getNanos());
        kafkaProducer.send(hubEventAvro, hubEventAvro.getHubId(), timestamp, topic);
    }
}
