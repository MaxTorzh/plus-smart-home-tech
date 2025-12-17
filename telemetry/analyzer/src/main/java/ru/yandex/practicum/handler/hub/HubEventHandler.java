package ru.yandex.practicum.handler.hub;

import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;

/**
 * HubEventHandler is an interface for handling hub events in the smart home system.
 * Implementations of this interface process different types of hub events such as
 * device additions, removals, and scenario changes.
 */
public interface HubEventHandler {
    String getType();

    void handle(HubEventAvro event);
}
