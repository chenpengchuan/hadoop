package com.hadoop.rocketmq.consumer;

import com.hadoop.rocketmq.config.RocketConfig;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * create by pengchuan.chen on 2020/4/17
 */
public class Consumer {

  public static void consumer() throws MQClientException {

    // Instantiate with specified consumer group name.
    DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(RocketConfig.MQ_GROUP);

    //set consume from first offset
    consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

    // Specify name server addresses.
    consumer.setNamesrvAddr(RocketConfig.MQ_NAME_SERVER);

    // Subscribe one more more topics to consume.
    consumer.subscribe(RocketConfig.MQ_TOPIC, RocketConfig.MQ_TAGES);
    // Register callback to execute on arrival of messages fetched from brokers.
    consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
      System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
      return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    });

    //Launch the consumer instance.
    consumer.start();

    System.out.println("Consumer Starting......");
  }


  /**
   * 顺序消息消费，带事务方式（应用可控制Offset什么时候提交）
   */
  public static void orderedConsumer() throws Exception {
    DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(RocketConfig.MQ_GROUP);
    consumer.setNamesrvAddr(RocketConfig.MQ_NAME_SERVER);

    /**
     * 设置Consumer第一次启动是从队列头部开始消费还是队列尾部开始消费,
     * 如果非第一次启动，那么按照上次消费的位置继续消费
     */
    consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

    consumer.subscribe(RocketConfig.MQ_TOPIC, "TagA || TagC || TagD");

    consumer.registerMessageListener(new MessageListenerOrderly() {

      AtomicLong consumeTimes = new AtomicLong(0);

      @Override
      public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
        context.setAutoCommit(false);
        System.out.printf(Thread.currentThread().getName() + " Receive New Messages: " + msgs + "%n");
        this.consumeTimes.incrementAndGet();
        if ((this.consumeTimes.get() % 2) == 0) {
          return ConsumeOrderlyStatus.SUCCESS;
        } else if ((this.consumeTimes.get() % 3) == 0) {
          return ConsumeOrderlyStatus.ROLLBACK;
        } else if ((this.consumeTimes.get() % 4) == 0) {
          return ConsumeOrderlyStatus.COMMIT;
        } else if ((this.consumeTimes.get() % 5) == 0) {
          context.setSuspendCurrentQueueTimeMillis(3000);
          return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
        }
        return ConsumeOrderlyStatus.SUCCESS;
      }
    });

    consumer.start();

    System.out.printf("Consumer Started.%n");
  }

  public static void consumerInOrder() throws Exception {
    DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(RocketConfig.MQ_GROUP);
    consumer.setInstanceName(System.currentTimeMillis() + "_instanceName");
//    consumer.setConsumeThreadMin(3);
    consumer.setNamesrvAddr(RocketConfig.MQ_NAME_SERVER);
    /**
     * 设置Consumer第一次启动是从队列头部开始消费还是队列尾部开始消费
     * 如果非第一次启动，那么按照上次消费的位置继续消费
     */
    consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

    consumer.subscribe(RocketConfig.MQ_TOPIC, "*");

    consumer.registerMessageListener(new MessageListenerOrderly() {

      Random random = new Random();

      @Override
      public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
        //是否设置自动commit
        context.setAutoCommit(true);
        for (MessageExt msg : msgs) {
          String content = new String(msg.getBody());
          System.out.println(msg.getKeys());
          // 可以看到每个queue有唯一的consume线程来消费, 订单对每个queue(分区)有序
          System.out.println("consumeThread=" + Thread.currentThread().getName() + ",queueId=" + msg.getQueueId() + ", content:" + content);
        }

        try {
          //模拟业务逻辑处理中...
          TimeUnit.SECONDS.sleep(random.nextInt(10));
        } catch (Exception e) {
          e.printStackTrace();
        }
        return ConsumeOrderlyStatus.SUCCESS;
      }
    });

    consumer.start();

    System.out.println("Consumer Started.");
  }

  public static void main(String[] args) throws Exception {
    //消费。
//    consumer();

    //顺序消费
//    orderedConsumer();
    consumerInOrder();
  }
}
