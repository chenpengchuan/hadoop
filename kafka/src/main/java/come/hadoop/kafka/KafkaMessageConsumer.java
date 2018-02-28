package come.hadoop.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class KafkaMessageConsumer {
    private final String TOPIC = "logkeeper";
    private final String BROKERLIST = "192.168.2.204:9092";
    private final String GROUPID = "test1";
    private final String ZOOKEEPER = "192.168.2.204:2181/carpo/kafka";

    private void consumerMessage() {
        Properties props = new Properties();
//        props.put("zookeeper.connect", ZOOKEEPER);
        props.put("bootstrap.servers", BROKERLIST);
        props.put("group.id", GROUPID);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000"); //设置超时时间30s
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(TOPIC));
//        consumer.subscribe(Arrays.asList(TOPIC, "bar"));
        System.out.println("Subscribed to topic " + TOPIC);
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
        }
    }

    public static void main(String[] args) {
        new KafkaMessageConsumer().consumerMessage();
    }
}
