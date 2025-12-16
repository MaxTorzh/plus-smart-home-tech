package ru.yandex.practicum.telemetry.aggregator.service;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.telemetry.aggregator.repository.SnapshotRepository;

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
        if (event == null || event.getHubId() == null || event.getId() == null) {
            log.warn("Invalid event received: {}", event);
            return Optional.empty();
        }

        log.debug("Processing event: hub={}, sensor={}, time={}, data={}",
                event.getHubId(), event.getId(), event.getTimestamp(), event.getPayload());
        SensorsSnapshotAvro snapshot = snapshots.findByHubId(event.getHubId())
                .orElseGet(() -> createNewSnapshot(event));

        if (!needsUpdate(snapshot, event)) {
            log.debug("No update needed for sensor={} in hub={}",
                    event.getId(), event.getHubId());
            return Optional.empty();
        }

        SensorsSnapshotAvro updatedSnapshot = updateSnapshot(snapshot, event);
        snapshots.save(updatedSnapshot);
        log.info("Snapshot updated for hub={}, sensor={}, data={}",
                event.getHubId(), event.getId(), event.getPayload());
        return Optional.of(updatedSnapshot);
    }

    /**
     * Checks if sensor state needs updating based on timestamp and data changes.
     */
    private boolean needsUpdate(SensorsSnapshotAvro snapshot, SensorEventAvro event) {
        SensorStateAvro currentState = snapshot.getSensorsState().get(event.getId());

        if (currentState == null) {
            return true;
        }
        if (event.getTimestamp().isBefore(currentState.getTimestamp())) {
            return false;
        }
        return !currentState.getData().equals(event.getPayload());
    }

    /**
     * Creates updated snapshot with new sensor state.
     */
    private SensorsSnapshotAvro updateSnapshot(SensorsSnapshotAvro snapshot, SensorEventAvro event) {
        Map<String, SensorStateAvro> newStates = new HashMap<>(snapshot.getSensorsState());

        SensorStateAvro newState = SensorStateAvro.newBuilder()
                .setTimestamp(event.getTimestamp())
                .setData(event.getPayload())
                .build();

        newStates.put(event.getId(), newState);

        Instant newTimestamp = snapshot.getTimestamp().isAfter(event.getTimestamp())
                ? snapshot.getTimestamp()
                : event.getTimestamp();

        return SensorsSnapshotAvro.newBuilder(snapshot)
                .setTimestamp(newTimestamp)
                .setSensorsState(newStates)
                .build();
    }

    /**
     * Creates new snapshot for a hub with initial sensor state.
     */
    private SensorsSnapshotAvro createNewSnapshot(SensorEventAvro event) {
        log.info("Creating new snapshot for hub={}", event.getHubId());

        Map<String, SensorStateAvro> initialState = new HashMap<>();
        initialState.put(event.getId(), SensorStateAvro.newBuilder()
                .setTimestamp(event.getTimestamp())
                .setData(event.getPayload())
                .build());

        return SensorsSnapshotAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp())
                .setSensorsState(initialState)
                .build();
    }
}