package ru.yandex.practicum.handler.hub.implementation;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.handler.hub.HubEventHandler;
import ru.yandex.practicum.kafka.telemetry.event.DeviceAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.mapper.Mapper;
import ru.yandex.practicum.repository.SensorRepository;

@Component
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
public class DeviceAddedHandler implements HubEventHandler {
    final SensorRepository sensorRepository;
    final Mapper mapper;

    @Override
    public String getType() {
        return DeviceAddedEventAvro.class.getSimpleName();
    }

    @Override
    @Transactional
    public void handle(HubEventAvro event) {
        sensorRepository.save(mapper.toSensor(event));
    }
}
