package come.hadoop.kafka.produce;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

/**
 * bijection-avro方式
 * create by pengchuan.chen on 2020/1/2
 */
public class BijectionSendMessageAvro {
  private static final String TOPIC = "avro6";
  private static final String BROKERLIST = "192.168.2.200:9092";
  private static final String SCHEMA_STR = "{\"namespace\":\"kafka_datatype\",\"type\":\"record\",\"name\":\"Customer\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"name\",\"type\":[\"null\",\"string\"]},{\"name\":\"chengji\",\"type\":[\"null\",\"double\"]},{\"name\":\"times\",\"type\":\"long\"}]}";

  public static void main(String[] args) throws Exception {
    Properties props = new Properties();
    props.put("bootstrap.servers", BROKERLIST);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(SCHEMA_STR);
    Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);
    Producer<String, byte[]> producer = new KafkaProducer<>(props);

    for (int i = 5; i < 7; i++) {
      double chengji = ThreadLocalRandom.current().nextDouble(50);

      GenericRecord avroRecord = new GenericData.Record(schema);
      avroRecord.put("id", 1000 + i);
      avroRecord.put("name", null);
      avroRecord.put("chengji", null);
      avroRecord.put("times", new Date().getTime());
//      avroRecord.put("date", new SimpleDateFormat("yyyy-MM-dd").format(new Date()));
      byte[] avroRecordBytes = recordInjection.apply(avroRecord);
      ProducerRecord<String, byte[]> record = new ProducerRecord<>(TOPIC, avroRecordBytes);
      producer.send(record).get();
    }
  }

}
