package ru.yandex.practicum.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.telemetry.event.ClimateSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.LightSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.MotionSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.kafka.telemetry.event.SwitchSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.TemperatureSensorAvro;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * SensorSnapshotService is responsible for managing and updating sensor state snapshots.
 * It maintains a collection of sensor states grouped by hub ID and determines when
 * sensor states have changed based on incoming sensor events.
 *
 * This service tracks sensor state changes and creates snapshots that represent
 * the current state of all sensors within a hub. It prevents duplicate or outdated
 * events from affecting the sensor state and only produces snapshots when actual
 * changes occur.
 */
@Slf4j
@Service
public class SensorSnapshotService {
    private final Map<String, SensorsSnapshotAvro> sensorsSnapshotMap = new HashMap<>();

    /**
     * Updates the state of a sensor based on a new event and returns an updated snapshot
     * if the state has changed or was newly added.
     *
     * This method performs several checks:
     * - Ensures the event timestamp is not older than the existing state
     * - Compares the new payload with the existing one to detect changes
     * - Only returns a snapshot if there's an actual change in state
     *
     * @param event the sensor event containing new state information
     * @return an Optional containing the updated snapshot if state changed, empty otherwise
     */
    public Optional<SensorsSnapshotAvro> updateState(SensorEventAvro event) {
        String hubId = event.getHubId();
        String sensorId = event.getId();
        Instant timestamp = event.getTimestamp();

        SensorsSnapshotAvro snapshot = sensorsSnapshotMap.computeIfAbsent(hubId, k ->
                new SensorsSnapshotAvro(hubId, timestamp, new HashMap<>()));
        Map<String, SensorStateAvro> sensorsState = snapshot.getSensorsState();
        SensorStateAvro oldState = sensorsState.get(sensorId);

        if (oldState != null) {
            Instant oldTimestamp = oldState.getTimestamp();

            if (oldTimestamp.isAfter(timestamp)) {
                log.warn("Sensor event is missed from sensor id: {} with old timestamp: {} > new timestamp: {}",
                        sensorId, oldTimestamp, timestamp);
                return Optional.empty();
            }

            if (isUnchanged(oldState.getData(), event.getPayload())) {
                log.info("Sensor {} state unchanged", sensorId);
                return Optional.empty();
            }

            log.debug("Sensor {} state changed", sensorId);
        } else {
            log.debug("Sensor {} state added", sensorId);
        }

        SensorStateAvro newState = new SensorStateAvro(timestamp, event.getPayload());
        sensorsState.put(sensorId, newState);
        snapshot.setTimestamp(timestamp);

        log.info("Sensor {} state updated", sensorId);
        return Optional.of(snapshot);
    }

    /**
     * Determines if two sensor payloads are equivalent (unchanged).
     *
     * This method compares different types of sensor payloads:
     * - Climate sensors: temperature, humidity, and CO2 levels
     * - Light sensors: link quality and luminosity
     * - Motion sensors: link quality, motion detection, and voltage
     * - Switch sensors: state
     * - Temperature sensors: Celsius and Fahrenheit temperatures
     *
     * @param oldPayload the previous sensor data
     * @param newPayload the new sensor data
     * @return true if payloads are equivalent, false otherwise
     */
    private Boolean isUnchanged(Object oldPayload, Object newPayload) {
        if (!oldPayload.getClass().equals(newPayload.getClass())) {
            log.warn("Payload type mismatch: {} != {}", oldPayload.getClass(), newPayload.getClass());
            return false;
        }

        switch (oldPayload) {
            case ClimateSensorAvro oldClimate when newPayload instanceof ClimateSensorAvro newClimate -> {
                log.info("Climate sensor state check");
                return oldClimate.getTemperatureC() == newClimate.getTemperatureC()
                        && oldClimate.getHumidity() == newClimate.getHumidity()
                        && oldClimate.getCo2Level() == newClimate.getCo2Level();
            }
            case LightSensorAvro oldLight when newPayload instanceof LightSensorAvro newLight -> {
                log.info("Light sensor state check");
                return oldLight.getLinkQuality() == newLight.getLinkQuality()
                        && oldLight.getLuminosity() == newLight.getLuminosity();
            }
            case MotionSensorAvro oldMotion when newPayload instanceof MotionSensorAvro newMotion -> {
                log.info("Motion sensor state check");
                return oldMotion.getLinkQuality() == newMotion.getLinkQuality()
                        && oldMotion.getMotion() == newMotion.getMotion()
                        && oldMotion.getVoltage() == newMotion.getVoltage();
            }
            case SwitchSensorAvro oldSwitch when newPayload instanceof SwitchSensorAvro newSwitch -> {
                log.info("Switch sensor state check");
                return oldSwitch.getState() == newSwitch.getState();
            }
            case TemperatureSensorAvro oldTemp when newPayload instanceof TemperatureSensorAvro neTemp -> {
                log.info("Temperature sensor state check");
                return oldTemp.getTemperatureC() == neTemp.getTemperatureC()
                        && oldTemp.getTemperatureF() == neTemp.getTemperatureF();
            }
            default -> {
                log.warn("Unknown payload type: {}", oldPayload.getClass());
                return false;
            }
        }
    }
}