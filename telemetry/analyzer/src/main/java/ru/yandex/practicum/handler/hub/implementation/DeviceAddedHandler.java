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

/**
 * DeviceAddedHandler is a hub event handler that processes DeviceAddedEventAvro events.
 * It saves new sensor information to the database when a device is added to a hub.
 */
@Component
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
public class DeviceAddedHandler implements HubEventHandler {
    final SensorRepository sensorRepository;
    final Mapper mapper;

    /**
     * Returns the event type that this handler processes.
     *
     * @return the simple class name of DeviceAddedEventAvro
     */
    @Override
    public String getType() {
        return DeviceAddedEventAvro.class.getSimpleName();
    }

    /**
     * Handles a DeviceAddedEvent by saving the new sensor to the database.
     *
     * @param event the hub event containing device addition information
     */
    @Override
    @Transactional
    public void handle(HubEventAvro event) {
        sensorRepository.save(mapper.toSensor(event));
    }
}
