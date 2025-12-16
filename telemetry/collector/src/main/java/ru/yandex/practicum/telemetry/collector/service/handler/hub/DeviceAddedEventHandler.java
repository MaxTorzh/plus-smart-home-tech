package ru.yandex.practicum.telemetry.collector.service.handler.hub;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.telemetry.collector.configuration.CollectorKafkaConfig;
import ru.yandex.practicum.grpc.telemetry.event.DeviceAddedEventProto;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.kafka.telemetry.event.DeviceAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceTypeAvro;
import ru.yandex.practicum.telemetry.collector.producer.KafkaEventProducer;

/**
 * Handler for Device Added Event from the Hub
 */
@Component
@Slf4j
public class DeviceAddedEventHandler extends BaseHubEventHandler<DeviceAddedEventAvro> {

    public DeviceAddedEventHandler(KafkaEventProducer producer, CollectorKafkaConfig config) {
        super(producer, config);
        log.info("DeviceAddedEventHandler instantiated.");
    }

    @Override
    public HubEventProto.PayloadCase getMessageType() {
        return HubEventProto.PayloadCase.DEVICE_ADDED;
    }

    @Override
    protected DeviceAddedEventAvro mapToAvro(final HubEventProto event) {
        log.debug("Mapping DeviceAddedEventProto to Avro: {}", event);
        if (event.getPayloadCase() != HubEventProto.PayloadCase.DEVICE_ADDED) {
            throw new IllegalArgumentException(
                    "Invalid payload type for DeviceAddedEventHandler: " + event.getPayloadCase());
        }
        final DeviceAddedEventProto _event = event.getDeviceAdded();
        final DeviceTypeAvro deviceType = DeviceTypeAvro.valueOf(_event.getType().name());
        return DeviceAddedEventAvro.newBuilder()
                .setId(_event.getId())
                .setType(deviceType)
                .build();
    }
}
