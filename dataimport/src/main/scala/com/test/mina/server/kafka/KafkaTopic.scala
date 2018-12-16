package com.test.mina.server.kafka

import java.util.Properties

import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.ZkConnection
import com.test.mina.server.utils.{ConfigManager, LogSupport}
import kafka.admin.RackAwareMode
import kafka.admin.RackAwareMode.Safe
import kafka.admin.AdminUtils
import kafka.utils.ZkUtils
import java.util.concurrent.TimeUnit


trait KafkaTopic extends LogSupport {
  // 连接配置
  val ZK_HOST = ConfigManager.zooKeeperServer
  val KAFKAHOST = ConfigManager.kafkaBroker
  // session过期时间
  val SEESSION_TIMEOUT = ConfigManager.kafkaSessionTimeout * 1000
  // 连接超时时间
  val CONNECT_TIMEOUT = ConfigManager.kafkaConnectTimeout * 1000
  // kafka集群是否是安全cluster
  val isSecureKafkaCluster = ConfigManager.kafkaSecureKafkaCluster
  val rackAwareMode: RackAwareMode = Safe

  // zookeeper client
  val zkClient: ZkClient = new ZkClient(
    ZK_HOST,
    SEESSION_TIMEOUT,
    CONNECT_TIMEOUT)
  zkClient.setZkSerializer(new MyZkSerialier())

  lazy val zkUtils: ZkUtils = new ZkUtils(zkClient, new ZkConnection(ZK_HOST), isSecureKafkaCluster)

  def fillProducerProperties(): Properties = {
    val props = new Properties()
//    props.put(TopicConfig.FILE_DELETE_DELAY_MS_CONFIG,"60000")
//    props.put("serializer.class", "kafka.serializer.StringEncoder")
//    props.put("partitioner.class", classOf[HashPartitioner].getName)
//    props.put("producer.type", "sync")
//    props.put("batch.num.messages", "1")
//    props.put("queue.buffering.max.messages", "1000000")
//    props.put("queue.enqueue.timeout.ms", "20000000")
    props
  }

  lazy val topicConfig: Properties = fillProducerProperties()

  /**
    * 创建主题
    *
    * @param topic      主题名称
    * @param partition  分区数
    * @param repilcate  副本数
    * @param properties 配置信息
    */
  @throws[InterruptedException]
  def createTopic(topicName: String, nRetriesLeft: Int = 3, partitions: Int = 10, replication: Int = 3): Unit = {
    if(!AdminUtils.topicExists(zkUtils, topicName)) {
      try{
        log.info("Creating topic " + topicName)
        AdminUtils.createTopic(zkUtils, topicName, partitions, replication, topicConfig, rackAwareMode)
      }
      catch {
        case e: Exception =>
          log.error("Topic create failed due to " + e.toString)
          if (nRetriesLeft <= 0) throw new RuntimeException("Failed to create topic \"" + topicName + "\". Is Kafka and Zookeeper running?")
          else {
            log.info("Failed to create topic, trying again in 5 seconds...")
            TimeUnit.SECONDS.sleep(5)
            createTopic(topicName, nRetriesLeft - 1, partitions, replication)
          }
      }
    } else log.info(s"topic: $topicName is exeist.")

  }

  /**
    * 删除主题
    *
    * @param topicName
    */
  def deleteTopic(topicName: String): Unit = {
    var timeOut = 10 // 最多等待10s
    if (AdminUtils.topicExists(zkUtils, topicName)) {
      log.info("Deleting topic " + topicName)
      AdminUtils.deleteTopic(zkUtils, topicName)
      while (timeOut >= 0 && AdminUtils.topicExists(zkUtils, topicName)) {
        log.info("Waiting for kafka to really delete topic ...")
        TimeUnit.SECONDS.sleep(1)
        timeOut -= 1
      }
    }
  }

}

