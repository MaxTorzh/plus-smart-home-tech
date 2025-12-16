package deserializer;

import exception.DeserializationException;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * BaseAvroDeserializer is a generic Kafka deserializer for Avro specific records.
 *
 * This abstract deserializer provides the basic functionality for deserializing
 * Avro-encoded messages from Kafka topics using binary decoding. It can be extended
 * or used directly with a specific Avro schema to deserialize messages into
 * SpecificRecordBase objects.
 *
 * @param <T> the type of SpecificRecordBase to deserialize to
 */
public class BaseAvroDeserializer<T extends SpecificRecordBase> implements Deserializer<T> {
    private final DecoderFactory decoderFactory;
    private final DatumReader<T> reader;

    public BaseAvroDeserializer(Schema schema) {
        this(DecoderFactory.get(), schema);
    }

    public BaseAvroDeserializer(DecoderFactory decoderFactory, Schema schema) {
        this.decoderFactory = decoderFactory;
        this.reader = new SpecificDatumReader<>(schema);
    }

    /**
     * Deserializes Avro-encoded data from a Kafka message.
     *
     * @param topic the topic from which the message was consumed
     * @param data the Avro-encoded binary data to deserialize
     * @return the deserialized Avro object, or null if data is null
     * @throws DeserializationException if an error occurs during deserialization
     */
    @Override
    public T deserialize(String topic, byte[] data) {
        try {
            if (data != null) {
                BinaryDecoder decoder = decoderFactory.binaryDecoder(data, null);
                return this.reader.read(null, decoder);
            }

            return null;
        } catch (Exception e) {
            throw new DeserializationException("Error deserializing data from topic " + topic, e);
        }
    }
}
