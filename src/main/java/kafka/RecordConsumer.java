package kafka;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.awaitility.Awaitility.given;

public class RecordConsumer {

    private static final Logger log = LoggerFactory.getLogger(RecordConsumer.class);
    private Consumer<String, GenericRecord> consumer;
    private AtomicBoolean fetchMessage = new AtomicBoolean();
    private GenericRecord consumedRecord;

    private RecordConsumer() {
        init();
    }

    public void init() {
        KafkaConfigs consumerConfig = new KafkaConfigs();
        consumer = new KafkaConsumer<>(consumerConfig.getConsumerConfig());
    }

    public static RecordConsumer createConsumer() {
        return new RecordConsumer();
    }

    public GenericRecord consumeAvroRecord(String topicName, String expectedValue, String schemaPath) throws IOException {
        consumer.subscribe(Collections.singleton(topicName));
        try {
            poll(expectedValue, schemaPath);
        } finally {
            consumer.close();
        }
        return consumedRecord;
    }

    public void poll(String expectedValue, String schemaPath) throws IOException {
        String schemaString = null;
        try (FileInputStream inputStream = new FileInputStream(schemaPath)) {
            schemaString = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
        }
        Schema schema = new Schema.Parser().parse(schemaString);

        given().await().atMost(15, TimeUnit.SECONDS).until(() -> {
            while (!fetchMessage.get()) {
                ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ZERO);
                for (ConsumerRecord<String, GenericRecord> record : records) {
                    log.info("getting record: {}", record);
                    if(schema.getFields().equals(record.value().getSchema().getFields())) {
                        if (record.value().toString().contains(expectedValue)) {
                            log.info("consumer record value for topic {} in partition {} and offset {} with key {} is: {}",
                                    record.topic(), record.partition(), record.offset(), record.key(), record.value());
                            consumedRecord = record.value();
                            fetchMessage.set(true);
                        }
                    }
                }
            }
            return true;
        });
    }

}
