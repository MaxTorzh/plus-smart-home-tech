package ru.yandex.practicum.telemetry.aggregator.repository;

import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.util.Optional;

/**
 * Repository interface for managing sensor snapshot data.
 * Provides methods for saving and retrieving snapshots based on the hub ID.
 */
public interface SnapshotRepository {

    void save(SensorsSnapshotAvro snapshot);

    Optional<SensorsSnapshotAvro> findByHubId(String hubId);
}
