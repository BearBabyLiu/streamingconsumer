package com.test.mina.server.kafka

import java.util.Properties
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import com.test.mina.server.utils.{ConfigManager, LogSupport}


class HuaChengProducer extends KafkaTopic with LogSupport {
  private val retryTimes = ConfigManager.kafkaRetryTimes
  private val batchSize = ConfigManager.kafkaBatchSize
  private val linerMs = ConfigManager.kafkaLinerMs
  private val bufSize = ConfigManager.kafkaBufferMemory
  // 生产者的配置
  val props = new Properties()
  props.put("bootstrap.servers", KAFKAHOST)
  props.put("acks", "all")
  props.put("retries", retryTimes.toString)
  props.put("batch.size", batchSize.toString)
  props.put("linger.ms", linerMs.toString)
  props.put("buffer.memory", bufSize.toString)
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  def getProducer: KafkaProducer[String, String] = {
    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](props)
    producer
  }

  def send(producer: KafkaProducer[String, String], topicName: String, key: String, context: String): Unit = {
    producer.send(new ProducerRecord[String, String](topicName, key, context))
  }

}

object HuaChengProducer {
  def apply(): HuaChengProducer = new HuaChengProducer()
  val producer = apply.getProducer
}
