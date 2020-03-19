import com.github.javafaker.Faker;
import com.integrations.domain.event.schema.PageViewEvent;
import constants.KafkaConstants;
import kafka.RecordConsumer;
import kafka.RecordProducer;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.RecordObjectMapper;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

public class KafkaTests {

    private static final Logger log = LoggerFactory.getLogger(KafkaTests.class);
    private RecordObjectMapper recordObjectMapper = new RecordObjectMapper();
    private Faker faker = new Faker();

    @Test
    public void i_send_message_to_kafka_producer_then_consumer_receives() throws IOException {

        // create an object
        PageViewEvent expectedPageViewEvent = PageViewEvent.newBuilder()
            .setItemId(faker.number().digits(8))
            .setItemTitle(faker.commerce().productName())
            .setScrollRange(Integer.parseInt(faker.number().digits(2)))
            .setStayTerm(Long.parseLong(faker.number().digits(2)))
            .setScrollUpDownCount(Integer.parseInt(faker.number().digits(2)))
            .build();

        // create and produce the avro record
        RecordProducer recordProducer = RecordProducer.createProducer();
        recordProducer.sendAvroRecord(KafkaConstants.TOPIC_NAME, "test-key", expectedPageViewEvent, KafkaConstants.SCHEMA_PATH);

        // let's start the consumer and retrieve the records
        RecordConsumer recordConsumer = RecordConsumer.createConsumer();
        GenericRecord actualRecord = recordConsumer.consumeAvroRecord(KafkaConstants.TOPIC_NAME,expectedPageViewEvent.getItemId().toString(), KafkaConstants.SCHEMA_PATH);

        // verify our expected vs actual objects :bloody-heart-pumping:
        log.info("actual record: {}", actualRecord);
        log.info("expected record: {}", expectedPageViewEvent);
        PageViewEvent actualPageViewEvent = recordObjectMapper.mapRecordToObject(actualRecord, new PageViewEvent());
        log.info("actual page view event is {}", actualPageViewEvent);

        assertThat(actualPageViewEvent).isEqualToComparingFieldByField(expectedPageViewEvent);
    }
}
