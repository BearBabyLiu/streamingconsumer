package scala.com.bigdata

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

/**
  * Created by Administrator on 2018/11/10.
  */
object KafkaConsumer {
  val conf = new SparkConf().setMaster("local[2]").setAppName("KafkaCosumer")
  val ssc = new StreamingContext(conf, Seconds(1))

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092,anotherhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "kafka-consumer",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  val topics = Array("topicA", "topicB")
  val stream = KafkaUtils.createDirectStream[String, String](
    ssc,
    PreferConsistent,
    Subscribe[String, String](topics, kafkaParams)
  )
}
