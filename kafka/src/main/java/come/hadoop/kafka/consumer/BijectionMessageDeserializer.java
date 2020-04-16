package come.hadoop.kafka.consumer;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

/**
 * bijection-avro方式
 * create by pengchuan.chen on 2020/1/2
 */
public class BijectionMessageDeserializer {

  private static final String TOPIC = "register1";
  private static final String BROKERLIST = "192.168.2.200:9092";
  private static final String SCHEMA_STR = "{\"namespace\":\"customerManagement.register1\",\"type\":\"record\",\"name\":\"Customer\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"age\",\"type\":\"int\"}]}";


  public static void main(String[] args) {
    Properties props = new Properties();
    props.put("bootstrap.servers", BROKERLIST);
    props.put("group.id", "group1");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props);
    consumer.subscribe(Collections.singletonList(TOPIC));
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(SCHEMA_STR);
    Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);
    // 设置分区开头读取, 0表示立即返回，无需等待
    consumer.seekToBeginning(consumer.poll(0).partitions());
    while (true) {
      ConsumerRecords<String, byte[]> records = consumer.poll(100);
      for (ConsumerRecord<String, byte[]> record : records) {
        GenericRecord genericRecord = recordInjection.invert(record.value()).get();
        System.out.println("value = ["
                        + "student.id = " + genericRecord.get("id")
                        + ",student.name = " + genericRecord.get("name")
                        + ",student.age = " + genericRecord.get("age")
                        + "],"
                        + "partition = " + record.partition() + ",offset = " + record.offset()
        );
      }

      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

}
