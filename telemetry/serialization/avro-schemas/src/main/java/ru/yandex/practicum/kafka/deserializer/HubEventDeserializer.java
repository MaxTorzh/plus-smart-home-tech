package ru.yandex.practicum.kafka.deserializer;

import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;

/**
 * A deserializer for converting binary data into {@link HubEventAvro} objects using Avro schema.
 * @see BaseAvroDeserializer
 * @see HubEventAvro
 */

public class HubEventDeserializer extends BaseAvroDeserializer<HubEventAvro> {

    public HubEventDeserializer() {
        super(HubEventAvro.getClassSchema());
    }
}
