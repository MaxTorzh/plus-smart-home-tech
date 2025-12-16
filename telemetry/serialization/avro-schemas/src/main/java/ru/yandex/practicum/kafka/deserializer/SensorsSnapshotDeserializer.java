package ru.yandex.practicum.kafka.deserializer;

import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

/**
 * A deserializer for converting binary data into {@link SensorsSnapshotAvro} objects using Avro schema.
 * @see BaseAvroDeserializer
 * @see SensorsSnapshotAvro
 */

public class SensorsSnapshotDeserializer extends BaseAvroDeserializer<SensorsSnapshotAvro> {

    public SensorsSnapshotDeserializer() {
        super(SensorsSnapshotAvro.getClassSchema());
    }
}
