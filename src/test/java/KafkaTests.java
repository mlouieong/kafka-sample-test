import kafka.RecordConsumer;
import kafka.RecordProducer;
import com.github.javafaker.Faker;
import model.PageViewEvent;
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
        String schemaPath = "src/test/resources/avro-schemas/page-view-event.avsc";
        String topicName = "page-view-event";

        // create an object
        PageViewEvent expectedPageViewEvent = new PageViewEvent();
        expectedPageViewEvent.setItemId(faker.number().digits(8));
        expectedPageViewEvent.setItemTitle(faker.commerce().productName());
        expectedPageViewEvent.setScrollRange(Integer.parseInt(faker.number().digits(2)));
        expectedPageViewEvent.setStayTerm(Long.parseLong(faker.number().digits(2)));
        expectedPageViewEvent.setScrollUpDownCount(Integer.parseInt(faker.number().digits(2)));

        // let's start the consumer
        RecordConsumer recordConsumer = RecordConsumer.createConsumer();

        // crete and produce the avro record
        RecordProducer recordProducer = RecordProducer.createProducer();
        recordProducer.sendAvroRecord(topicName, "test-key", expectedPageViewEvent, schemaPath);

        // fetch
        GenericRecord actualRecord = recordConsumer.consumeAvroRecord(topicName,expectedPageViewEvent.getItemId(), schemaPath);

        // verify
        log.info("actual record: {}", actualRecord);
        log.info("expected record: {}", expectedPageViewEvent);
        PageViewEvent actualPageViewEvent = recordObjectMapper.mapRecordToObject(actualRecord, new PageViewEvent());
        log.info("actual page view event is {}", actualPageViewEvent);

        assertThat(actualPageViewEvent).isEqualToComparingFieldByField(expectedPageViewEvent);
    }
}
