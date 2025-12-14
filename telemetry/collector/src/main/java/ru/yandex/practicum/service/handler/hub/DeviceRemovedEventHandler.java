package ru.yandex.practicum.service.handler.hub;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.configuration.CollectorKafkaConfig;
import ru.yandex.practicum.grpc.telemetry.event.DeviceRemovedEventProto;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.kafka.telemetry.event.DeviceRemovedEventAvro;
import ru.yandex.practicum.producer.KafkaEventProducer;

/**
 * Handler for Device Removed event from the Hub.
 */
@Component
@Slf4j
public class DeviceRemovedEventHandler extends BaseHubEventHandler<DeviceRemovedEventAvro> {

    public DeviceRemovedEventHandler(KafkaEventProducer producer, CollectorKafkaConfig config) {
        super(producer, config);
        log.info("DeviceRemovedEventHandler instantiated.");
    }

    @Override
    public HubEventProto.PayloadCase getMessageType() {
        return HubEventProto.PayloadCase.DEVICE_REMOVED;
    }

    @Override
    protected DeviceRemovedEventAvro mapToAvro(final HubEventProto event) {
        log.debug("Mapping DeviceRemovedEventProto to Avro: {}", event);
        if (event.getPayloadCase() != HubEventProto.PayloadCase.DEVICE_REMOVED) {
            throw new IllegalArgumentException(
                    "Invalid payload type for DeviceRemovedEventHandler: " + event.getPayloadCase());
        }
        final DeviceRemovedEventProto _event = event.getDeviceRemoved();
        return DeviceRemovedEventAvro.newBuilder()
                .setId(_event.getId())
                .build();
    }
}
