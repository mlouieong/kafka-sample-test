package kafka;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import util.PropertyFileReader;

import java.util.Properties;

public class KafkaConfigs {

    private PropertyFileReader propertyFileReader = new PropertyFileReader();
    private final String KAFKA_BOOTSTRAP_SERVER = propertyFileReader.getProperty("KAFKA_BOOTSTRAP_SERVER");
    private final String SCHEMA_REGISTRY_URL_CONFIG = "schema.registry.url";
    private final String SCHEMA_REGISTRY_URL = propertyFileReader.getProperty("SCHEMA_REGISTRY_URL");
    private final String CLIENT_ID = "test-client-id1";

    private Properties properties;

    public KafkaConfigs() {
        this.properties = new Properties();
    }

    private Properties getCommonConfig() {
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVER);
        properties.put(SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
        properties.put(CommonClientConfigs.CLIENT_ID_CONFIG, CLIENT_ID);
        return properties;
    }

    public Properties getProducerConfig() {
        properties.putAll(getCommonConfig());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
//        properties.put(ProducerConfig.ACKS_CONFIG, "0");
//        properties.put(ProducerConfig.RETRIES_CONFIG, "0");
        return properties;
    }

    public Properties getConsumerConfig() {
        properties.putAll(getCommonConfig());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-id1");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return properties;
    }
}
