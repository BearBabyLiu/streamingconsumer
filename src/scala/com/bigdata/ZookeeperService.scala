package scala.com.bigdata

import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo, TopicMetadataRequest}
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkMarshallingError
import org.I0Itec.zkclient.serialize.ZkSerializer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.HasOffsetRanges

/**
  * Created by Administrator on 2018/11/10.
  */
case class ZookeeperService(topic: String, zkServers: String) extends LogSupport{

  val zkTopicPath: String = {
    val topicDirs = new ZKGroupTopicDirs("streaming_consumer_group", topic)
    val zkTopicPath = s"${topicDirs.consumerOffsetDir}"
    zkTopicPath
  }

  /**
    * 获取kafka的partition与其对应Leader
    * @param client zkClient
    * @param brokerHost brokerHost 主机名：比如master
    * @param brokerPort brokerPort 端口：9092
    * @return Map[Int, String] key:partitionId value:leadHost
    */
  def  getPartitionLeader(client: ZkClient, brokerHost: String, brokerPort: Int): Map[Int, String] = {

    val children = client.countChildren(zkTopicPath)
    if(children > 0) {
      val req = new TopicMetadataRequest(List(topic), 0)
      val getLeaderConsumer = new SimpleConsumer(brokerHost, brokerPort, 10000, 10000, "OffsetLookup")
      val res = getLeaderConsumer.send(req)
      val topicMetaOption = res.topicsMetadata.headOption
      val partitions = topicMetaOption match {
        case Some(tm) => {
          tm.partitionsMetadata.map(pm => (pm.partitionId, pm.leader.get.host)).toMap[Int, String]
        }
        case None => Map[Int, String]() }
      partitions
    } else {
      Map[Int, String]()
    }
  }

  /**
    * 创建zk的客户端
    * @return
    */
  def createZkClient(): ZkClient = {
    val zkClient = new ZkClient(zkServers,60000,60000,new ZkSerializer {
      override def serialize(data: Object): Array[Byte] = {
        try {
          return data.toString.getBytes("UTF-8")
        }catch {
          case e: ZkMarshallingError => null
        }
      }

      override def deserialize(bytes: Array[Byte]): AnyRef = {
        try {
          return new String(bytes,"UTF-8")
        }
        catch {
          case e: ZkMarshallingError => null
        } } })
    zkClient
  }

  /**
    * 计算cosumer的偏移，从哪里开始消费
    * @param brokerHost brokerHost 主机名：比如master
    * @param brokerPort brokerPort 端口：9092
    * @return
    */
  def calculateCunsumerOffset(brokerHost: String, brokerPort: Int): Map[TopicPartition, Long] = {
    val client = createZkClient()
    try {
      val partitionLead = getPartitionLeader(client,brokerHost, brokerPort)
      if(partitionLead.nonEmpty) {
        var fromOffsets: Map[TopicPartition, Long] = Map()
        partitionLead.keys.toList.foreach(partitionId => {
          val partitionOffset = client.readData[String](s"$zkTopicPath/$partitionId")
          val tp = TopicAndPartition(topic, partitionId)

          /** 获取topic partition 最小偏移 */
          val requestMin = OffsetRequest(Map(tp -> PartitionOffsetRequestInfo(OffsetRequest.EarliestTime, 1)))
          val consumerMin = new SimpleConsumer(partitionLead(partitionId), 9092, 10000, 10000, "getMinOffset")
          val minOffset = consumerMin.getOffsetsBefore(requestMin).partitionErrorAndOffsets(tp).offsets.head

          /** 获取topic partition 最大偏移 */
          val requestMax = OffsetRequest(Map(tp -> PartitionOffsetRequestInfo(OffsetRequest.LatestTime, 1)))
          val consumerMax = new SimpleConsumer(partitionLead(partitionId), 9092, 10000, 10000, "getMaxOffset")
          val maxOffset = consumerMax.getOffsetsBefore(requestMax).partitionErrorAndOffsets(tp).offsets.head

          /** zk记录的偏移 */
          var offsetFromZk = partitionOffset.toLong

          if(minOffset > 0 && offsetFromZk < minOffset) {
            offsetFromZk = minOffset
          }

          if(maxOffset > 0 && offsetFromZk > maxOffset) {
            offsetFromZk = maxOffset
          }

          fromOffsets += (new TopicPartition(tp.topic, tp.partition) -> offsetFromZk)
        })
        fromOffsets
      } else {
        Map[TopicPartition, Long]()
      }
    } catch {
      case e: Exception => {
        logError(s"calculateCunsumerOffset exception", e)
        Map[TopicPartition, Long]()
      }
    } finally {
      client.close()
    }

  }

  /**
    * 保存RDD每个分区的偏移到ZK
    * @param rdd
    */
  def savePartitionOffsets(rdd: RDD[ConsumerRecord[String,String]]): Unit ={
    val client = createZkClient()
    try {
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      for(o <- offsetRanges) {
        ZkUtils(client, false).updatePersistentPath(s"${zkTopicPath}/${o.partition}",String.valueOf(o.untilOffset))
      }
    }catch {
      case e: Exception => {
        logError(s"savePartitionOffsets exception", e)
      }
    } finally {
      client.close()
    }
  }
}
