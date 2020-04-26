package com.hadoop.rocketmq.consumer;

import com.hadoop.rocketmq.config.RocketConfig;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * create by pengchuan.chen on 2020/4/17
 */
public class Consumer2 {

  /**
   * 顺序消费队列消息
   * @throws Exception
   */
  public static void consumerInOrder() throws Exception {
    DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(RocketConfig.MQ_GROUP);
    consumer.setInstanceName(System.currentTimeMillis()+"_instanceName");
    consumer.setNamesrvAddr(RocketConfig.MQ_NAME_SERVER);
    /**
     * 设置Consumer第一次启动是从队列头部开始消费还是队列尾部开始消费
     * 如果非第一次启动，那么按照上次消费的位置继续消费
     */
    consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

    consumer.subscribe(RocketConfig.MQ_TOPIC, "TagA || TagB || TagC");

    consumer.registerMessageListener(new MessageListenerOrderly() {

      Random random = new Random();

      @Override
      public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
        //是否设置自动commit
        context.setAutoCommit(true);
        String content ="";
        String flag =Long.toString(System.currentTimeMillis());
        for (MessageExt msg : msgs) {
          content = new String(msg.getBody());
          // 可以看到每个queue有唯一的consume线程来消费, 订单对每个queue(分区)有序
          System.out.println("consumeThread=" + Thread.currentThread().getName() + ",queueId=" + msg.getQueueId() + ", content:" + new String(msg.getBody())+" flag:"+flag);
        }

        try {
          //模拟业务逻辑处理中...
          TimeUnit.SECONDS.sleep(random.nextInt(10));
          System.out.println("--consumeThread="+ Thread.currentThread().getName()+ ", content:" + content+" finished flag:"+flag);
        } catch (Exception e) {
          e.printStackTrace();
        }
        return ConsumeOrderlyStatus.SUCCESS;
      }
    });

    consumer.start();

    System.out.println("Consumer2 Started.");
  }

  public static void main(String[] args) throws Exception {
    consumerInOrder();
  }
}
