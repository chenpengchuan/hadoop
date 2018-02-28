在本机hosts文件中配置上kafka机器对应的ip和域名

运行生产实例：
修改变量：TOPIC，BROKERLIST的值
在idea中直接运行主类:KafkaProduce.java

运行消费实例：
修改变量：TOPIC，BROKERLIST,以及GROUPID的值
在idea中直接运行主类:KafkaMessageConsumer.java