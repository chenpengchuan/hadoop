package come.hadoop.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaProduce {
    private final String TOPIC = "logkeeper";
    private final String BROKERLIST = "192.168.2.204:9092";

    private void priduceMessage() {
        Properties props = new Properties();
        //此处配置的是kafka的brodelist的端口
        props.put("bootstrap.servers", BROKERLIST);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //创建生产这对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        String message = "hello";
        for (int i = 0; i < 10; i++) {
            //生成消息
            ProducerRecord data = new ProducerRecord<String, String>(TOPIC, Integer.toString(i), message + Integer.toString(i));
            //发送消息
            producer.send(data);
        }
        producer.close();
        System.out.println("send finished");
    }

    public static void main(String[] args) {
        new KafkaProduce().priduceMessage();
    }
}
