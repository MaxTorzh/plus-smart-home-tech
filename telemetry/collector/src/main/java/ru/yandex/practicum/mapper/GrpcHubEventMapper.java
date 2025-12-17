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

/**
 * GrpcHubEventMapper is a utility component that converts gRPC HubEventProto messages
 * to Avro HubEventAvro objects for Kafka serialization.
 *
 * This mapper handles the conversion between Protocol Buffer representations used
 * in gRPC communication and Avro representations used for Kafka messaging.
 */
@Component
public class GrpcHubEventMapper {

    /**
     * Converts a HubEventProto gRPC message to a HubEventAvro Avro object.
     *
     * @param event the gRPC hub event to convert
     * @return the converted Avro hub event
     * @throws IllegalArgumentException if the event payload type is not supported
     */
    public HubEventAvro toAvro(HubEventProto event) {
        Object payload = switch (event.getPayloadCase()) {
            case DEVICE_ADDED -> toDeviceAddedAvro(event.getDeviceAdded());
            case SCENARIO_ADDED -> toScenarioAddedAvro(event.getScenarioAdded());
            case DEVICE_REMOVED -> toDeviceRemovedAvro(event.getDeviceRemoved());
            case SCENARIO_REMOVED -> toScenarioRemovedAvro(event.getScenarioRemoved());
            default -> throw new IllegalArgumentException("Event type not found " + event.getClass().getName());
        };
        Instant timestamp = Instant.ofEpochSecond(event.getTimestamp().getSeconds(), event.getTimestamp().getNanos());

        return HubEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(timestamp)
                .setPayload(payload)
                .build();
    }

    /**
     * Converts a DeviceAddedEventProto to DeviceAddedEventAvro.
     *
     * @param event the gRPC device added event to convert
     * @return the converted Avro device added event
     */
    private DeviceAddedEventAvro toDeviceAddedAvro(DeviceAddedEventProto event) {
        return DeviceAddedEventAvro.newBuilder()
                .setId(event.getId())
                .setType(DeviceTypeAvro.valueOf(event.getType().name()))
                .build();
    }

    /**
     * Converts a ScenarioAddedEventProto to ScenarioAddedEventAvro.
     *
     * @param event the gRPC scenario added event to convert
     * @return the converted Avro scenario added event
     */
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

    /**
     * Converts a DeviceRemovedEventProto to DeviceRemovedEventAvro.
     *
     * @param event the gRPC device removed event to convert
     * @return the converted Avro device removed event
     */
    private DeviceRemovedEventAvro toDeviceRemovedAvro(DeviceRemovedEventProto event) {
        return DeviceRemovedEventAvro.newBuilder()
                .setId(event.getId())
                .build();
    }

    /**
     * Converts a ScenarioRemovedEventProto to ScenarioRemovedEventAvro.
     *
     * @param event the gRPC scenario removed event to convert
     * @return the converted Avro scenario removed event
     */
    private ScenarioRemovedEventAvro toScenarioRemovedAvro(ScenarioRemovedEventProto event) {
        return ScenarioRemovedEventAvro.newBuilder()
                .setName(event.getName())
                .build();
    }

    /**
     * Maps a DeviceActionProto to DeviceActionAvro.
     *
     * @param action the gRPC device action to convert
     * @return the converted Avro device action
     */
    private DeviceActionAvro mapDeviceActionAvro(DeviceActionProto action) {
        return DeviceActionAvro.newBuilder()
                .setType(ActionTypeAvro.valueOf(action.getType().name()))
                .setSensorId(action.getSensorId())
                .setValue(action.getValue())
                .build();
    }

    /**
     * Maps a ScenarioConditionProto to ScenarioConditionAvro.
     *
     * @param condition the gRPC scenario condition to convert
     * @return the converted Avro scenario condition
     */
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
