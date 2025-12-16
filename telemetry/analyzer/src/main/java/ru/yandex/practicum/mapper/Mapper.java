package ru.yandex.practicum.mapper;

import com.google.protobuf.Timestamp;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionRequest;
import ru.yandex.practicum.kafka.telemetry.event.ActionTypeAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.model.Action;
import ru.yandex.practicum.model.Sensor;

import java.time.Instant;

/**
 * Mapper is a utility component that converts between different data models
 * used in the telemetry analyzer service, such as Avro events, Proto messages,
 * and JPA entities.
 */
@Component
public class Mapper {
    /**
     * Converts an Action entity to a DeviceActionRequest proto message.
     *
     * @param action the action entity to convert
     * @return a DeviceActionRequest proto message
     */
    public DeviceActionRequest toActionRequest(Action action) {
        return DeviceActionRequest.newBuilder()
                .setHubId(action.getScenario().getHubId())
                .setScenarioName(action.getScenario().getName())
                .setAction(toDeviceActionProto(action))
                .setTimestamp(toTimestamp())
                .build();
    }

    /**
     * Converts an Action entity to a DeviceActionProto message.
     *
     * @param action the action entity to convert
     * @return a DeviceActionProto message
     */
    private DeviceActionProto toDeviceActionProto(Action action) {
        return DeviceActionProto.newBuilder()
                .setSensorId(action.getSensor().getId())
                .setType(toActionTypeProto(action.getType()))
                .setValue(action.getValue())
                .build();
    }

    /**
     * Converts an ActionTypeAvro enum to ActionTypeProto enum.
     *
     * @param actionType the Avro action type to convert
     * @return the corresponding Proto action type
     */
    private ActionTypeProto toActionTypeProto(ActionTypeAvro actionType) {
        return switch (actionType) {
            case ACTIVATE -> ActionTypeProto.ACTIVATE;
            case DEACTIVATE -> ActionTypeProto.DEACTIVATE;
            case INVERSE -> ActionTypeProto.INVERSE;
            case SET_VALUE -> ActionTypeProto.SET_VALUE;
        };
    }

    /**
     * Converts a HubEventAvro containing a DeviceAddedEvent to a Sensor entity.
     *
     * @param event the hub event containing device addition information
     * @return a Sensor entity
     */
    public Sensor toSensor(HubEventAvro event) {
        DeviceAddedEventAvro deviceAddedEvent = (DeviceAddedEventAvro) event.getPayload();
        return Sensor.builder().id(deviceAddedEvent.getId()).hubId(event.getHubId()).build();
    }

    /**
     * Creates a current timestamp as a protobuf Timestamp message.
     *
     * @return the current timestamp
     */
    private Timestamp toTimestamp() {
        Instant instant = Instant.now();
        return Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }
}
