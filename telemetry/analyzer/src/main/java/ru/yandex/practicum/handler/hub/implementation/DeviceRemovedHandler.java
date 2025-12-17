package ru.yandex.practicum.handler.hub.implementation;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.handler.hub.HubEventHandler;
import ru.yandex.practicum.kafka.telemetry.event.DeviceRemovedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.repository.SensorRepository;

/**
 * DeviceRemovedHandler is a hub event handler that processes DeviceRemovedEventAvro events.
 * It handles device removal events from hubs.
 */
@Component
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
public class DeviceRemovedHandler implements HubEventHandler {
    final SensorRepository sensorRepository;

    /**
     * Returns the event type that this handler processes.
     *
     * @return the simple class name of DeviceRemovedHandler
     */
    @Override
    public String getType() {
        return DeviceRemovedHandler.class.getSimpleName();
    }

    /**
     * Handles a DeviceRemovedEvent by finding the sensor in the database.
     *
     * @param event the hub event containing device removal information
     */
    @Override
    @Transactional
    public void handle(HubEventAvro event) {
        DeviceRemovedEventAvro deviceRemovedAvro = (DeviceRemovedEventAvro) event.getPayload();
        sensorRepository.findByIdAndHubId(deviceRemovedAvro.getId(), event.getHubId());
    }
}
