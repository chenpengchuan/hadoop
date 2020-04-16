package come.hadoop.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

/**
 * Schema-Registry 方式,以string方式反序列化
 * create by pengchuan.chen on 2019/12/31
 */
public class ReadMessageAvroDeserializer {
  private static final String TOPIC = "register1";
  private static final String BROKERLIST = "192.168.2.200:9092";
  private static final String REGISTRY_RUL = "http://192.168.2.200:8081";

  @SuppressWarnings("deprecation")
  public static void main(String[] args) {
    // Properties 对象
    Properties props = new Properties();
    props.put("bootstrap.servers", BROKERLIST);
    props.put("group.id", "group1");    // 消费者群组
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
    props.put("schema.registry.url", REGISTRY_RUL);

    // consumer 对象
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

    // 订阅主题
    consumer.subscribe(Collections.singletonList(TOPIC));    // 支持订阅多个主题，也支持正则

    try {
      // 设置分区开头读取, 0表示立立即返回，无需等待
      consumer.seekToBeginning(consumer.poll(0).partitions());
      while (true) {
        // 0.1s 的轮询等待
        ConsumerRecords<String, String> records = consumer.poll(100);
        System.out.println(records.count());
        for (ConsumerRecord<String, String> record : records) {
          // 输出到控制台
          System.out.printf("offset = %s, value = %s\n",
                  record.offset(), record.value());
        }
        // 同步提交偏移量
        consumer.commitSync();
        Thread.sleep(3000);
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      consumer.close();
    }
  }
}
