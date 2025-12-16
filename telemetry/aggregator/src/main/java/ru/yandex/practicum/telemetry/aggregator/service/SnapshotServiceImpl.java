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
        log.info("Updating snapshot for hubId={} with event - sensorId={}", event.getHubId(), event.getId());

        final SensorsSnapshotAvro snapshot = snapshots.findByHubId(event.getHubId())
                .orElseGet(() -> buildSnapshot(event));

        if (isCurrentSnapshotValid(snapshot, event)) {
            log.info("No update required for hubId={} and sensorId={}", event.getHubId(), event.getId());
            return Optional.empty();
        }

        updateSnapshotData(snapshot, event);

        snapshots.save(snapshot);
        return Optional.of(snapshot);
    }

    private void updateSnapshotData(final SensorsSnapshotAvro snapshot, final SensorEventAvro event) {
        log.debug("Updating snapshot with new sensor state {}", event.getPayload());
        final SensorStateAvro newState = SensorStateAvro.newBuilder()
                .setTimestamp(event.getTimestamp())
                .setData(event.getPayload())
                .build();

        snapshot.getSensorsState().put(event.getId(), newState);
        snapshot.setTimestamp(event.getTimestamp());
    }

    private boolean isCurrentSnapshotValid(final SensorsSnapshotAvro currentSnapshot,
                                           final SensorEventAvro event) {
        log.debug("Validating current snapshot data {} against new event data {}",currentSnapshot, event);
        final SensorStateAvro currentState = currentSnapshot.getSensorsState().get(event.getId());

        return currentState != null &&
                (!currentState.getTimestamp().isBefore(event.getTimestamp()) ||
                        currentState.getData().equals(event.getPayload()));
    }

    private SensorsSnapshotAvro buildSnapshot(final SensorEventAvro event) {
        log.debug("Creating new snapshot with reading from the sensor event {}.", event);
        return SensorsSnapshotAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp())
                .setSensorsState(new HashMap<>())
                .build();
    }
}
