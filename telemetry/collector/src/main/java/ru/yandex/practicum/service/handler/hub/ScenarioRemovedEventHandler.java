package ru.yandex.practicum.service.handler.hub;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.configuration.CollectorKafkaConfig;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.ScenarioRemovedEventProto;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioRemovedEventAvro;
import ru.yandex.practicum.producer.KafkaEventProducer;

/**
 * Handler for Removing Automated Scenario event from the Hub.
 */
@Component
@Slf4j
public class ScenarioRemovedEventHandler extends BaseHubEventHandler<ScenarioRemovedEventAvro> {

    public ScenarioRemovedEventHandler(KafkaEventProducer producer, CollectorKafkaConfig config) {
        super(producer, config);
        log.info("ScenarioRemovedEventHandler instantiated.");
    }

    @Override
    public HubEventProto.PayloadCase getMessageType() {
        return HubEventProto.PayloadCase.SCENARIO_REMOVED;
    }

    @Override
    protected ScenarioRemovedEventAvro mapToAvro(final HubEventProto event) {
        log.debug("Mapping ScenarioRemovedEventProto to Avro: {}", event);
        if (event.getPayloadCase() != HubEventProto.PayloadCase.SCENARIO_REMOVED) {
            throw new IllegalArgumentException(
                    "Invalid payload type for ScenarioRemovedEventHandler: " + event.getPayloadCase());
        }
        final ScenarioRemovedEventProto _event = event.getScenarioRemoved();
        return ScenarioRemovedEventAvro.newBuilder()
                .setName(_event.getName())
                .build();
    }
}
