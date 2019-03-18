import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.StickyAssignor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.security.scram.ScramLoginModule;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Date;
import java.util.Properties;

public class KafkaClient {
    private KafkaConsumer<String, String> consumer;
    private Producer<String, String> producer;

    private String brokers;
    private String username;
    private String password;
    private String clientId;
    private String groupeId;
    private String registry;
    private String topic;

    public KafkaClient() {
        brokers = System.getenv("KAFKA_BROKERS");
        username = System.getenv("KAFKA_USERNAME");
        password = System.getenv("KAFKA_PASSWORD");
        clientId = System.getenv("KAFKA_CLIENTID");
        groupeId = System.getenv("KAFKA_GROUPID");
        registry = System.getenv("KAFKA_REGISTRY");
        topic = System.getenv("KAFKA_TOPIC");
        client();
    }

    private void client(){
        Properties props = new Properties();

        // Connection parameters
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupeId);

        // How to deserialize Avro messages
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, registry);
        props.put(AbstractKafkaAvroSerDeConfig.AUTO_REGISTER_SCHEMAS, Boolean.FALSE.toString());
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, Boolean.TRUE.toString());

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.FALSE.toString());
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, StickyAssignor.class.getName());

        // Add security options
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "SCRAM-SHA-512");
        props.put("sasl.jaas.config", credentials(username, password));

        consumer = new KafkaConsumer(props);
        producer = new KafkaProducer(props);
    }

    private String credentials(String username, String password) {
        return String.format(
                "%s required username=\"%s\" password=\"%s\";",
                ScramLoginModule.class.getName(),
                username,
                password
        );
    }

    public void consume() {
        consumer.subscribe(Arrays.asList(topic));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            records.forEach(record -> {
                System.out.printf("%s [%d] offset=%d, key=%s, value=\"%s\"\n",
                  record.topic(), record.partition(),
                  record.offset(), record.key(), record.value());
			});
        }
    }

    public void produce() {
        Thread one = new Thread() {
            public void run() {
                try {
                    int i = 0;
                    while(true) {
                        Date d = new Date();
                        producer.send(new ProducerRecord(topic, Integer.toString(i), d.toString()));
                        Thread.sleep(1000);
                        i++;
                    }
                } catch (InterruptedException v) {
                    System.out.println(v);
                }
            }
        };
        one.start();
    }
}
