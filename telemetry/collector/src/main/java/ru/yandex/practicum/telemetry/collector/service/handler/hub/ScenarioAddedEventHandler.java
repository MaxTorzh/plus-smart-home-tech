package ru.yandex.practicum.telemetry.collector.service.handler.hub;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.telemetry.collector.configuration.CollectorKafkaConfig;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.ScenarioAddedEventProto;
import ru.yandex.practicum.grpc.telemetry.event.ScenarioConditionProto;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.telemetry.collector.producer.KafkaEventProducer;

import java.util.List;
import java.util.function.Function;

/**
 * Handler for Added new Automated Scenario event from the Hub.
 */
@Component
@Slf4j
public class ScenarioAddedEventHandler extends BaseHubEventHandler<ScenarioAddedEventAvro> {

    public ScenarioAddedEventHandler(KafkaEventProducer producer, CollectorKafkaConfig config) {
        super(producer, config);
        log.info("ScenarioAddedEventHandler instantiated.");
    }

    @Override
    public HubEventProto.PayloadCase getMessageType() {
        return HubEventProto.PayloadCase.SCENARIO_ADDED;
    }

    @Override
    protected ScenarioAddedEventAvro mapToAvro(final HubEventProto event) {
        log.debug("Mapping ScenarioAddedEventProto to Avro: {}", event);
        if (event.getPayloadCase() != HubEventProto.PayloadCase.SCENARIO_ADDED) {
            throw new IllegalArgumentException(
                    "Invalid payload type for ScenarioAddedEventHandler: " + event.getPayloadCase());
        }
        final ScenarioAddedEventProto _event = event.getScenarioAdded();

        return ScenarioAddedEventAvro.newBuilder()
                .setName(_event.getName())
                .setActions(mapListToAvro(_event.getActionList(), this::mapToAvro))
                .setConditions(mapListToAvro(_event.getConditionList(), this::mapToAvro))
                .build();
    }

    private DeviceActionAvro mapToAvro(final DeviceActionProto action) {
        return DeviceActionAvro.newBuilder()
                .setSensorId(action.getSensorId())
                .setType(ActionTypeAvro.valueOf(action.getType().name()))
                .build();
    }

    private ScenarioConditionAvro mapToAvro(final ScenarioConditionProto condition) {

        final ScenarioConditionAvro.Builder builder = ScenarioConditionAvro.newBuilder()
                .setSensorId(condition.getSensorId())
                .setType(ConditionTypeAvro.valueOf(condition.getType().name()))
                .setOperation(ConditionOperationAvro.valueOf(condition.getOperation().name()));

        switch (condition.getValueCase()) {
            case BOOL_VALUE:
                builder.setValue(condition.getBoolValue());
                break;
            case INT_VALUE:
                builder.setValue(condition.getIntValue());
                break;
            default:
                builder.setValue(null);
        }

        return builder.build();
    }

    private <T, R> List<R> mapListToAvro(List<T> source, Function<T, R> mapper) {
        if (source == null || source.isEmpty()) {
            return List.of();
        }
        return source.stream()
                .map(mapper)
                .toList();
    }
}
