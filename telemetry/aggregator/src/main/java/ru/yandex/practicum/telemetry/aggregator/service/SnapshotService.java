package ru.yandex.practicum.telemetry.aggregator.service;

import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.util.Optional;

/**
 * Service interface for managing sensor snapshot updates.
 */
public interface SnapshotService {

    Optional<SensorsSnapshotAvro> updateState(SensorEventAvro event);
}