package com.hadoop.rocketmq.producer;

import com.hadoop.rocketmq.config.RocketConfig;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.io.IOException;

/**
 * create by pengchuan.chen on 2020/4/17
 */
public class Producer {

  /**
   * Producer端同步发送消息
   *
   * @throws Exception
   */
  public static void syncProducer() throws Exception {
    //Instantiate with a producer group name.
    DefaultMQProducer producer = new DefaultMQProducer(RocketConfig.MQ_GROUP);
    producer.setRetryAnotherBrokerWhenNotStoreOK(true);
    // Specify name server addresses.
    producer.setNamesrvAddr(RocketConfig.MQ_NAME_SERVER);
    //Launch the instance.
    producer.start();
    for (int i = 0; i < 10; i++) {
      final int index = i;
      //Create a message instance, specifying topic, tag and message body.
      Message msg = new Message(RocketConfig.MQ_TOPIC/* Topic */,
              RocketConfig.MQ_TAGES,
              RocketConfig.MQ_KEYS /* Tag */,
              ("Hello syncProducer message " + i).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
      );
      //Call send message to deliver message to one of brokers.
      SendResult sendResult = producer.send(msg);
      System.out.printf("%s%n", sendResult);
    }
    //Shut down once the producer instance is not longer in use.
    producer.shutdown();
  }

  /**
   * Producer端异步发送消息,
   * 异步消息通常用在对响应时间敏感的业务场景，即发送端不能容忍长时间地等待Broker的响应。
   *
   * @throws Exception
   */
  public static void asyncProducer() throws Exception {
    //Instantiate with a producer group name.
    DefaultMQProducer producer = new DefaultMQProducer(RocketConfig.MQ_GROUP);
    // Specify name server addresses.
    producer.setNamesrvAddr(RocketConfig.MQ_NAME_SERVER);
    //Launch the instance.
    producer.setRetryTimesWhenSendAsyncFailed(3);
    producer.start();
    for (int i = 0; i < 5; i++) {
      final int index = i;
      //Create a message instance, specifying topic, tag and message body.
      Message msg = new Message(RocketConfig.MQ_TOPIC,
              RocketConfig.MQ_TAGES,
              RocketConfig.MQ_KEYS,
              ("Hello asyncProducer message " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
      producer.send(msg, new SendCallback() {
        @Override
        public void onSuccess(SendResult sendResult) {
          System.out.printf("%-3d OK %s %n", index, sendResult.getMsgId());
        }

        @Override
        public void onException(Throwable e) {
          System.out.printf("%-3d Exception %s %n", index, e);
          e.printStackTrace();
        }
      });
    }
    //Shut down once the producer instance is not longer in use.
    producer.shutdown();
  }

  /**
   * 单向发送消息,这种方式主要用在不特别关心发送结果的场景，例如日志发送。
   *
   * @throws Exception
   */
  public static void onewayProducer() throws Exception {
    //Instantiate with a producer group name.
    DefaultMQProducer producer = new DefaultMQProducer(RocketConfig.MQ_GROUP);
    // Specify name server addresses.
    producer.setNamesrvAddr(RocketConfig.MQ_NAME_SERVER);
    //Launch the instance.
    producer.start();
    for (int i = 0; i < 5; i++) {
      //Create a message instance, specifying topic, tag and message body.
      Message msg = new Message(RocketConfig.MQ_TOPIC /* Topic */,
              RocketConfig.MQ_TAGES /* Tag */,
              ("Hello onewayProducer message " + i).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
      );
      //Call send message to deliver message to one of brokers.
      producer.sendOneway(msg);

    }
    //Shut down once the producer instance is not longer in use.
    producer.shutdown();
  }


  /**
   * 顺序发送消息.
   *
   * @throws Exception
   */
  public static void orderedProducer() throws MQClientException, IOException {
    String[] executionIds = new String[]{"execution1", "execution2", "execution3", "execution4"};
    String[] tags = new String[]{"TagA", "TagB", "TagC"};
    //Instantiate with a producer group name.
    MQProducer producer = new DefaultMQProducer(RocketConfig.MQ_GROUP);
    ((DefaultMQProducer) producer).setRetryAnotherBrokerWhenNotStoreOK(true);
    ((DefaultMQProducer) producer).setNamesrvAddr(RocketConfig.MQ_NAME_SERVER);
    ((DefaultMQProducer) producer).setCompressMsgBodyOverHowmuch(1024 * 1024 * 100);
    ((DefaultMQProducer) producer).setMaxMessageSize(1024*1024*100);
    //Launch the instance.
    producer.start();
//    byte[] bytes = FileUtils.getStreamBytes();
    for (int i = 0; i < 10; i++) {

      //Create a message instance, specifying topic, tag and message body.
      String executionId = executionIds[i % 4];
      String msgBody = "message executionId:" + executionId + "|tags:" + tags[i % 3] + "|keys:" + executionId;
      Message msg = new Message(RocketConfig.MQ_TOPIC, tags[i % 3], "keys" + executionId, msgBody.getBytes(RemotingHelper.DEFAULT_CHARSET));
      SendResult sendResult = null;
      try {
        sendResult = producer.send(msg, (mqs, msg1, arg) -> {
          Integer id = (Integer) arg;
          int index = id % mqs.size();
          return mqs.get(index);
        }, i);
      } catch (Exception e) {
        e.printStackTrace();
      }

      System.out.printf("%s%n", sendResult);
    }
    //server shutdown
    producer.shutdown();
  }

  public static void main(String[] args) throws Exception {
    //同步发送消息
//    syncProducer();

    //异步发送消息
//    asyncProducer();

//    //单向发送消息
//    onewayProducer();

    //顺序发送消息
    orderedProducer();
  }
}
