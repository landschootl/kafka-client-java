import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.security.scram.ScramLoginModule;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Date;
import java.util.Properties;

public class KafkaClient {
    String topic;
    KafkaConsumer<String, String> consumer;
    Producer<String, String> producer;

    public KafkaClient() {
        String brokers = System.getenv("KAFKA_BROKERS");
        String username = System.getenv("KAFKA_USERNAME");
        String password = System.getenv("KAFKA_PASSWORD");
        client(brokers, username, password);
    }

    private void client(String brokers, String username, String password){
        topic = username + "-default";
        String serializer = StringSerializer.class.getName();
        String deserializer = StringDeserializer.class.getName();
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("client.id", "your-app");
        props.put("group.id", username + "-consumer");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "earliest");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", deserializer);
        props.put("value.deserializer", deserializer);
        props.put("key.serializer", serializer);
        props.put("value.serializer", serializer);
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "SCRAM-SHA-512");
        props.put("sasl.jaas.config", credentials(username,password));
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
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("%s [%d] offset=%d, key=%s, value=\"%s\"\n",
                  record.topic(), record.partition(),
                  record.offset(), record.key(), record.value());
			}
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
