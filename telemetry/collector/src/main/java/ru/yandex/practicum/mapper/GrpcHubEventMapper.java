package ru.yandex.practicum.mapper;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceAddedEventProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceRemovedEventProto;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.ScenarioAddedEventProto;
import ru.yandex.practicum.grpc.telemetry.event.ScenarioConditionProto;
import ru.yandex.practicum.grpc.telemetry.event.ScenarioRemovedEventProto;
import ru.yandex.practicum.kafka.telemetry.event.ActionTypeAvro;
import ru.yandex.practicum.kafka.telemetry.event.ConditionOperationAvro;
import ru.yandex.practicum.kafka.telemetry.event.ConditionTypeAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceActionAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceRemovedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceTypeAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioConditionAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioRemovedEventAvro;

import java.time.Instant;
import java.util.List;

@Component
public class GrpcHubEventMapper {
    public HubEventAvro toAvro(HubEventProto event) {
        Object payload = switch (event.getPayloadCase()) {
            case DEVICE_ADDED -> toDeviceAddedAvro(event.getDeviceAdded());
            case SCENARIO_ADDED -> toScenarioAddedAvro(event.getScenarioAdded());
            case DEVICE_REMOVED -> toDeviceRemovedAvro(event.getDeviceRemoved());
            case SCENARIO_REMOVED -> toScenarioRemovedAvro(event.getScenarioRemoved());
            default -> throw new IllegalArgumentException("Не найден тип события " + event.getClass().getName());
        };
        Instant timestamp = Instant.ofEpochSecond(event.getTimestamp().getSeconds(), event.getTimestamp().getNanos());

        return HubEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(timestamp)
                .setPayload(payload)
                .build();
    }

    private DeviceAddedEventAvro toDeviceAddedAvro(DeviceAddedEventProto event) {
        return DeviceAddedEventAvro.newBuilder()
                .setId(event.getId())
                .setType(DeviceTypeAvro.valueOf(event.getType().name()))
                .build();
    }

    private ScenarioAddedEventAvro toScenarioAddedAvro(ScenarioAddedEventProto event) {
        List<ScenarioConditionAvro> conditions = event.getConditionList().stream()
                .map(this::mapScenarioConditionAvro)
                .toList();
        List<DeviceActionAvro> actions = event.getActionList().stream()
                .map(this::mapDeviceActionAvro)
                .toList();
        return ScenarioAddedEventAvro.newBuilder()
                .setName(event.getName())
                .setActions(actions)
                .setConditions(conditions)
                .build();
    }

    private DeviceRemovedEventAvro toDeviceRemovedAvro(DeviceRemovedEventProto event) {
        return DeviceRemovedEventAvro.newBuilder()
                .setId(event.getId())
                .build();
    }

    private ScenarioRemovedEventAvro toScenarioRemovedAvro(ScenarioRemovedEventProto event) {
        return ScenarioRemovedEventAvro.newBuilder()
                .setName(event.getName())
                .build();
    }

    private DeviceActionAvro mapDeviceActionAvro(DeviceActionProto action) {
        return DeviceActionAvro.newBuilder()
                .setType(ActionTypeAvro.valueOf(action.getType().name()))
                .setSensorId(action.getSensorId())
                .setValue(action.getValue())
                .build();
    }

    private ScenarioConditionAvro mapScenarioConditionAvro(ScenarioConditionProto condition) {
        Object value = switch (condition.getValueCase()) {
            case BOOL_VALUE -> condition.getBoolValue();
            case INT_VALUE -> condition.getIntValue();
            case VALUE_NOT_SET -> null;
        };
        return ScenarioConditionAvro.newBuilder()
                .setSensorId(condition.getSensorId())
                .setType(ConditionTypeAvro.valueOf(condition.getType().name()))
                .setValue(value)
                .setOperation(ConditionOperationAvro.valueOf(condition.getOperation().name()))
                .build();
    }
}
