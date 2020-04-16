package come.hadoop.kafka.produce;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Schema-Registry 方式
 * create by pengchuan.chen on 2019/12/30
 */
public class SendMessageAvro {

  private final String TOPIC = "register1";
  private final String BROKERLIST = "192.168.2.200:9092";
  //TODO 因为没有使用Avro生成对象，所以需要提供Avro Schema
  private final String SCHEMA_STR = "{\"namespace\":\"customerManagement.register1\",\"type\":\"record\",\"name\":\"Customer\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"age\",\"type\":\"int\"}]}";

  public static void main(String[] args) {
    new SendMessageAvro().sendMessage();
  }

  public void sendMessage() {
    // 创建生产者
    Properties kafkaProps = new Properties();
    // 指定broker
    kafkaProps.put("bootstrap.servers", BROKERLIST);
    // 设置序列化
    kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    kafkaProps.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
    kafkaProps.put("schema.registry.url", "http://192.168.2.200:8081");
    kafkaProps.put("group.id", "group1");

    @SuppressWarnings("resource")
    Producer<String, GenericRecord> producer = new KafkaProducer<String, GenericRecord>(kafkaProps);
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(SCHEMA_STR);


    // 发送多条消息
    for (int nCustomers = 10; nCustomers < 100; nCustomers++) {
      String name = "name" + nCustomers;
      int age = ThreadLocalRandom.current().nextInt(30);

      // ProducerRecord的值就是一个GenericRecord对象，它包含了shcema和数据
      GenericRecord customer = new GenericData.Record(schema);
      customer.put("id", nCustomers);
      customer.put("name", name);
      customer.put("age", age);

      // 创建ProducerRecord对象
      ProducerRecord<String, GenericRecord> data = new ProducerRecord<String,
              GenericRecord>(TOPIC, name, customer);

      try {
        // 发送消息
        producer.send(data, new DemoProducerCallback(nCustomers));
        Thread.sleep(10);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }
}

class DemoProducerCallback implements Callback {
  Integer id = null;

  public DemoProducerCallback(Integer id) {
    this.id = id;
  }

  @Override
  public void onCompletion(RecordMetadata recordMetadata, Exception e) {
    // TODO Auto-generated method stub
    if (e != null) {
      // 如果消息发送失败，打印异常
      e.printStackTrace();
    } else {
      System.out.println("Success send! Message ID: " + id);
    }
  }

}

