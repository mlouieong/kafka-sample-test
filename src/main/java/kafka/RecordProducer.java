package kafka;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.RecordObjectMapper;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class RecordProducer {

    private static final Logger log = LoggerFactory.getLogger(RecordProducer.class);
    private Producer<String, GenericRecord> producer;
    private RecordObjectMapper recordObjectMapper = new RecordObjectMapper();

    private RecordProducer() {
        init();
    }

    private void init() {
        KafkaConfigs producerConfig = new KafkaConfigs();
        producer = new KafkaProducer<>(producerConfig.getProducerConfig());
    }


    public static RecordProducer createProducer() {
        return new RecordProducer();
    }

    public void sendAvroRecord(String topicName, String key, Object object, String schemaPath) throws IOException {
        GenericRecord genericRecord = buildRecord(schemaPath, object);
        try {
            producer.send(new ProducerRecord<>(topicName, key, genericRecord), (recordMetadata, e) -> {
                if (e != null) {
                    log.error("Exception is found: {}", e.getMessage());
                } else {
                    log.info("producer record details for topic {} in partition {} with offset {} and timestamp {}",
                            recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp());
                }
            });
        } catch (SerializationException e) {
            log.error("Serialization exception is found: {}", e.getMessage());
        } finally {
            producer.flush();
            producer.close();
        }
    }

    private GenericRecord buildRecord(String schemaPath, Object object) throws IOException {
        String schemaString = null;
        try (FileInputStream inputStream = new FileInputStream(schemaPath)) {
            schemaString = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
        }
        Schema schema = new Schema.Parser().parse(schemaString);
        GenericRecord record = new GenericData.Record(schema);

        record = recordObjectMapper.mapObjectToRecord(schema, record, object);

        return record;
    }
}
