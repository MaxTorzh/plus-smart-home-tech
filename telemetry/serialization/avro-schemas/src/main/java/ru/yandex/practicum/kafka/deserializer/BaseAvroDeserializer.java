package ru.yandex.practicum.kafka.deserializer;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;

/**
 * A generic deserializer fir Avro messages that converts binary data to a specific Avro records.
 *  @param <T> the type of Avro record being deserialized, which must extend {@link org.apache.avro.specific.SpecificRecordBase}
 */

@Slf4j
public class BaseAvroDeserializer<T extends SpecificRecordBase> implements Deserializer<T> {

    private final Schema schema;
    private final DecoderFactory decoderFactory;

    public BaseAvroDeserializer(final Schema schema) {
        this.schema = schema;
        this.decoderFactory = DecoderFactory.get();
    }

    @Override
    public T deserialize(final String topic, final byte[] data) {
        if (data == null || data.length == 0) {
            log.warn("No data to deserialize for topic: {}", topic);
            return null;
        }

        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(data)) {
            BinaryDecoder decoder = decoderFactory.binaryDecoder(inputStream, null);
            DatumReader<T> datumReader = new SpecificDatumReader<>(schema);

            T result = datumReader.read(null, decoder);
            log.debug("Successfully deserialized record from topic: {}", topic);
            return result;
        } catch (IOException e) {
            log.error("Error during data deserialization for topic [{}]: {}", topic, e.getMessage());
            throw new SerializationException(
                    "Error occurs during data deserialization, topic [" + topic + "].", e);
        }
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public void close() {
    }
}
