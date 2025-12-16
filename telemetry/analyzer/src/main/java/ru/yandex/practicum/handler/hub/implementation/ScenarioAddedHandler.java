package ru.yandex.practicum.handler.hub.implementation;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.handler.hub.HubEventHandler;
import ru.yandex.practicum.kafka.telemetry.event.DeviceActionAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioConditionAvro;
import ru.yandex.practicum.model.Action;
import ru.yandex.practicum.model.Condition;
import ru.yandex.practicum.model.Scenario;
import ru.yandex.practicum.repository.ActionRepository;
import ru.yandex.practicum.repository.ConditionRepository;
import ru.yandex.practicum.repository.ScenarioRepository;
import ru.yandex.practicum.repository.SensorRepository;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * ScenarioAddedHandler is a hub event handler that processes ScenarioAddedEventAvro events.
 * It handles scenario creation events by saving scenarios, conditions, and actions to the database.
 */
@Component
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
public class ScenarioAddedHandler implements HubEventHandler {
    final ActionRepository actionRepository;
    final ConditionRepository conditionRepository;
    final ScenarioRepository scenarioRepository;
    final SensorRepository sensorRepository;

    /**
     * Returns the event type that this handler processes.
     *
     * @return the simple class name of ScenarioAddedEventAvro
     */
    @Override
    public String getType() {
        return ScenarioAddedEventAvro.class.getSimpleName();
    }

    /**
     * Handles a ScenarioAddedEvent by saving the scenario and its associated conditions and actions.
     *
     * @param event the hub event containing scenario addition information
     */
    @Override
    @Transactional
    public void handle(HubEventAvro event) {
        ScenarioAddedEventAvro scenarioAddedAvro = (ScenarioAddedEventAvro) event.getPayload();
        Optional<Scenario> scenario = scenarioRepository.findByHubIdAndName(event.getHubId(),
                scenarioAddedAvro.getName());

        Scenario scenarioEntity = scenario.orElseGet(() -> scenarioRepository.save(toScenario(event)));

        if (checkSensorInActions(scenarioAddedAvro, event.getHubId())) {
            actionRepository.saveAll(toActions(scenarioAddedAvro, scenarioEntity));
        }
        if (checkSensorInConditions(scenarioAddedAvro, event.getHubId())) {
            conditionRepository.saveAll(toConditions(scenarioAddedAvro, scenarioEntity));
        }
    }

    /**
     * Converts a hub event to a Scenario entity.
     *
     * @param event the hub event containing scenario information
     * @return a Scenario entity
     */
    private Scenario toScenario(HubEventAvro event) {
        ScenarioAddedEventAvro scenario = (ScenarioAddedEventAvro) event.getPayload();
        return Scenario.builder().name(scenario.getName()).hubId(event.getHubId()).build();
    }

    /**
     * Converts scenario conditions from the event to Condition entities.
     *
     * @param event the scenario added event
     * @param scenario the scenario entity to associate conditions with
     * @return a set of Condition entities
     */
    private Set<Condition> toConditions(ScenarioAddedEventAvro event, Scenario scenario) {
        return event.getConditions().stream()
                .map(condition -> Condition.builder()
                        .sensor(sensorRepository.findById(condition.getSensorId()).orElseThrow())
                        .scenario(scenario)
                        .type(condition.getType())
                        .operation(condition.getOperation())
                        .value(setValue(condition.getValue()))
                        .build())
                .collect(Collectors.toSet());
    }

    /**
     * Converts scenario actions from the event to Action entities.
     *
     * @param event the scenario added event
     * @param scenario the scenario entity to associate actions with
     * @return a set of Action entities
     */
    private Set<Action> toActions(ScenarioAddedEventAvro event, Scenario scenario) {
        return event.getActions().stream()
                .map(action -> Action.builder()
                        .sensor(sensorRepository.findById(action.getSensorId()).orElseThrow())
                        .scenario(scenario)
                        .type(action.getType())
                        .value(action.getValue())
                        .build())
                .collect(Collectors.toSet());
    }

    /**
     * Checks if all sensors referenced in conditions exist for the given hub.
     *
     * @param event the scenario added event
     * @param hubId the hub ID
     * @return true if all sensors exist, false otherwise
     */
    private Boolean checkSensorInConditions(ScenarioAddedEventAvro event, String hubId) {
        List<String> sensorIds = event.getConditions().stream()
                .map(ScenarioConditionAvro::getSensorId)
                .toList();
        return sensorRepository.existsByIdInAndHubId(sensorIds, hubId);
    }

    /**
     * Checks if all sensors referenced in actions exist for the given hub.
     *
     * @param event the scenario added event
     * @param hubId the hub ID
     * @return true if all sensors exist, false otherwise
     */
    private Boolean checkSensorInActions(ScenarioAddedEventAvro event, String hubId) {
        List<String> sensorIds = event.getActions().stream()
                .map(DeviceActionAvro::getSensorId)
                .toList();
        return sensorRepository.existsByIdInAndHubId(sensorIds, hubId);
    }

    /**
     * Sets the condition value, converting boolean values to integers.
     *
     * @param value the value to convert
     * @return integer representation of the value
     */
    private Integer setValue(Object value) {
        if (value instanceof Integer) {
            return (Integer) value;
        } else {
            return (Boolean) value ? 1 : 0;
        }
    }
}
