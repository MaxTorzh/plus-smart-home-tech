package ru.yandex.practicum.kafka.deserializer;

import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

/**
 * A deserializer for converting binary data into {@link SensorEventAvro} objects using Avro schema.
 * @see BaseAvroDeserializer
 * @see SensorEventAvro
 */

public class SensorEventDeserializer  extends BaseAvroDeserializer<SensorEventAvro> {

    public SensorEventDeserializer() {
        super(SensorEventAvro.getClassSchema());
    }
}
