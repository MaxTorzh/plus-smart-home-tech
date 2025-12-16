package ru.yandex.practicum.telemetry.aggregator.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.telemetry.aggregator.repository.SnapshotRepository;

import java.util.HashMap;
import java.util.Optional;

/**
 * Service implementation for managing sensor snapshots updates.
 */

@Component
@RequiredArgsConstructor
@Slf4j
public class SnapshotServiceImpl implements SnapshotService {

    private final SnapshotRepository snapshots;

    /**
     * Updates the state of a sensor snapshot based on the given sensor event.
     * <p>
     * This method performs the following operations:
     * <ol>
     *   <li>
     *     Attempts to find an existing snapshot for the hub associated with the event.
     *     If none is found, a new snapshot is created.
     *   </li>
     *   <li>
     *     Checks if the snapshot already contains sensor state data for the sensor event ID.
     *     If the current data is more recent or identical, the update is skipped, and
     *     {@code Optional.empty()} is returned.
     *   </li>
     *   <li>
     *     If the data needs to be updated, a new sensor state is created, and added
     *     to the snapshot. The snapshot timestamp is updated to match the event timestamp.
     *   </li>
     *   <li>
     *     The updated snapshot is saved to the repository,
     *     and the is returned in an {@code Optional}.
     *     </li>
     * </ol>
     * @param event the incoming sensor event
     * @return an {@code Optional} containing the updated snapshot or empty if no update was needed.
     *
     */
    @Override
    public Optional<SensorsSnapshotAvro> updateState(final SensorEventAvro event) {
        log.info("UPDATE_STATE - hub={}, sensor={}, time={}",
                event.getHubId(), event.getId(), event.getTimestamp());

        final SensorsSnapshotAvro snapshot = snapshots.findByHubId(event.getHubId())
                .orElseGet(() -> {
                    log.info("NEW_SNAPSHOT for hub={}", event.getHubId());
                    return buildSnapshot(event);
                });

        snapshot.setTimestamp(event.getTimestamp());

        updateSnapshotData(snapshot, event);

        snapshots.save(snapshot);

        log.info("SNAPSHOT_UPDATED - hub={}, sensors_count={}",
                snapshot.getHubId(), snapshot.getSensorsState().size());
        return Optional.of(snapshot);
    }

    private void updateSnapshotData(final SensorsSnapshotAvro snapshot, final SensorEventAvro event) {
        log.debug("Updating sensor state: {}", event.getId());

        final SensorStateAvro newState = SensorStateAvro.newBuilder()
                .setTimestamp(event.getTimestamp())
                .setData(event.getPayload())
                .build();

        snapshot.getSensorsState().put(event.getId(), newState);
    }

    private SensorsSnapshotAvro buildSnapshot(final SensorEventAvro event) {
        return SensorsSnapshotAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp())
                .setSensorsState(new HashMap<>())
                .build();
    }
}
