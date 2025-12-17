package ru.yandex.practicum.handler.hub.implementation;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.handler.hub.HubEventHandler;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioRemovedEventAvro;
import ru.yandex.practicum.model.Scenario;
import ru.yandex.practicum.repository.ActionRepository;
import ru.yandex.practicum.repository.ConditionRepository;
import ru.yandex.practicum.repository.ScenarioRepository;

import java.util.Optional;

/**
 * ScenarioRemovedHandler is a hub event handler that processes ScenarioRemovedEventAvro events.
 * It handles scenario removal events by deleting scenarios, conditions, and actions from the database.
 */
@Slf4j
@Component
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
public class ScenarioRemovedHandler implements HubEventHandler {
    final ActionRepository actionRepository;
    final ConditionRepository conditionRepository;
    final ScenarioRepository scenarioRepository;

    /**
     * Returns the event type that this handler processes.
     *
     * @return the simple class name of ScenarioRemovedHandler
     */
    @Override
    public String getType() {
        return ScenarioRemovedHandler.class.getSimpleName();
    }

    /**
     * Handles a ScenarioRemovedEvent by deleting the scenario and its associated conditions and actions.
     *
     * @param event the hub event containing scenario removal information
     */
    @Override
    @Transactional
    public void handle(HubEventAvro event) {
        ScenarioRemovedEventAvro scenarioRemovedAvro = (ScenarioRemovedEventAvro) event.getPayload();
        Optional<Scenario> scenario = scenarioRepository.findByHubIdAndName(event.getHubId(),
                scenarioRemovedAvro.getName());

        if (scenario.isPresent()) {
            Scenario scenarioEntity = scenario.get();
            actionRepository.deleteByScenario(scenarioEntity);
            conditionRepository.deleteByScenario(scenarioEntity);
            scenarioRepository.delete(scenarioEntity);
        } else {
            log.warn("Не найден сценарий {}", scenarioRemovedAvro.getName());
        }
    }
}
