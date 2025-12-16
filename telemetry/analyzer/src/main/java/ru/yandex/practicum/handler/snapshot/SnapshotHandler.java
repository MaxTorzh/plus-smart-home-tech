package ru.yandex.practicum.handler.snapshot;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.ClimateSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.LightSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.MotionSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.kafka.telemetry.event.SwitchSensorAvro;
import ru.yandex.practicum.model.Condition;
import ru.yandex.practicum.model.Scenario;
import ru.yandex.practicum.repository.ActionRepository;
import ru.yandex.practicum.repository.ConditionRepository;
import ru.yandex.practicum.repository.ScenarioRepository;
import ru.yandex.practicum.service.HubRouterClient;

import java.util.Map;
import java.util.Objects;

@Component
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
public class SnapshotHandler {
    final HubRouterClient hubRouterClient;
    final ConditionRepository conditionRepository;
    final ActionRepository actionRepository;
    final ScenarioRepository scenarioRepository;

    public void handleSnapshot(SensorsSnapshotAvro snapshot) {
        Map<String, SensorStateAvro> sensorState = snapshot.getSensorsState();
        scenarioRepository.findByHubId(snapshot.getHubId()).stream()
                .filter(scenario -> checkScenario(scenario, sensorState))
                .forEach(scenario -> {
                    sendAction(scenario);
                });
    }

    private Boolean checkOperation(Condition condition, Integer value) {
        Integer conditionValue = condition.getValue();

        switch (condition.getOperation()) {
            case EQUALS -> {
                return Objects.equals(value, conditionValue);
            }
            case GREATER_THAN -> {
                return value > conditionValue;
            }
            case LOWER_THAN -> {
                return value < conditionValue;
            }
            default -> {
                return false;
            }
        }
    }

    private Boolean checkCondition(Condition condition, Map<String, SensorStateAvro> sensorState) {
        SensorStateAvro sensorStateAvro = sensorState.get(condition.getSensor().getId());

        if (sensorStateAvro == null) return false;

        switch (condition.getType()) {
            case SWITCH -> {
                SwitchSensorAvro switchSensor = (SwitchSensorAvro) sensorStateAvro.getData();
                return checkOperation(condition, switchSensor.getState() ? 1 : 0);
            }
            case MOTION -> {
                MotionSensorAvro motionSensor = (MotionSensorAvro) sensorStateAvro.getData();
                return checkOperation(condition, motionSensor.getMotion() ? 1 : 0);
            }
            case HUMIDITY -> {
                ClimateSensorAvro humiditySensor = (ClimateSensorAvro) sensorStateAvro.getData();
                return checkOperation(condition, humiditySensor.getHumidity());
            }
            case TEMPERATURE -> {
                ClimateSensorAvro temperatureSensor = (ClimateSensorAvro) sensorStateAvro.getData();
                return checkOperation(condition, temperatureSensor.getTemperatureC());
            }
            case LUMINOSITY -> {
                LightSensorAvro lightSensor = (LightSensorAvro) sensorStateAvro.getData();
                return checkOperation(condition, lightSensor.getLuminosity());
            }
            case CO2LEVEL -> {
                ClimateSensorAvro co2Sensor = (ClimateSensorAvro) sensorStateAvro.getData();
                return checkOperation(condition, co2Sensor.getCo2Level());
            }

            default -> {
                return false;
            }
        }
    }

    private Boolean checkScenario(Scenario scenario, Map<String, SensorStateAvro> sensorState) {
        return conditionRepository.findAllByScenario(scenario).stream()
                .allMatch(condition -> checkCondition(condition, sensorState));
    }

    private void sendAction(Scenario scenario) {
        actionRepository.findAllByScenario(scenario).forEach(hubRouterClient::sendRequest);
    }
}
