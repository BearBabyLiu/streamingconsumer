package scala.com.bigdata

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Assign
import org.elasticsearch.spark.streaming.EsSparkStreaming

import scala.collection.mutable

/**
  * Created by Administrator on 2018/11/10.
  */
object KafkaConsumer {
  var kafkaStream: InputDStream[ConsumerRecord[String, String]] = null

  val conf = new SparkConf().setMaster("local[2]").setAppName("KafkaCosumer").
    set("es.nodes", "192.168.1.1").
    set("es.port", "9200").
    set("es.index.auto.create", "true")

  val ssc = new StreamingContext(conf, Seconds(1))

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "kafka-consumer",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  val topics = Array("topicName")
  val zkInstance = ZookeeperService("topicName", "master:2181")
  val fromOffsets = zkInstance.calculateCunsumerOffset("brokeHost", 9092)
  if (fromOffsets.isEmpty) {
    kafkaStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))
  } else {
    kafkaStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Assign[String, String](fromOffsets.keys.toList, kafkaParams, fromOffsets))
  }

  /**
    * kafka的数据入ES
    */
  kafkaStream.foreachRDD(rdd => {
    /**
      * json流也可以直接入es，也可以在这里转换
      */
    val message = rdd.map(record => record.value())
    val microbatches = mutable.Queue(message)
    val dstream = ssc.queueStream(microbatches)
    EsSparkStreaming.saveToEs(dstream, "spark/docs")
    zkInstance.savePartitionOffsets(rdd)
  })
}
