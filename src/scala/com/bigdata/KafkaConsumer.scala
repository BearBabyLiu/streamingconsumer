package scala.com.bigdata

import com.alibaba.fastjson.{JSON, JSONException}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Assign
import org.elasticsearch.spark.rdd.EsSpark

case class Employee(id: String, name: String, school: String, age: Int, adress: String)

/**
  * Created by Administrator on 2018/11/10.
  */
object KafkaConsumer extends LogSupport {
  def main(args: Array[String]): Unit = {

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
    val zkInstance = ZookeeperService("topicName", "localhost:2181")

    /** 从zk获取上一次保存的patition的offset,这里需要考虑最大值与最小值越界 */
    val fromOffsets = zkInstance.calculateCunsumerOffset("localhost", 9092)

    /**
      * 对于offset为0的情况则从头开始消费,否则从指定偏移消费
      */
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
    kafkaStream.foreachRDD(kafkaRDD => {
      if (!kafkaRDD.isEmpty()) {
        /** 取kafka的数据,数据是一个json字符串 */
        val lines = kafkaRDD.map(_.value())

        /** json解析为case class */
        val employeeRDD = lines.map(line => {
          var data: Employee = null
          try {
            data = JSON.parseObject(line, classOf[Employee])
          } catch {
            case e: JSONException =>
              logError(s"json parse error: $line", e)
          }
          logInfo(s"the employee data: $data")
          data
        })
        EsSpark.saveToEs(employeeRDD, "spark/docs")

        /** 每次写完ES后保存rdd的partition offset到zk */
        zkInstance.savePartitionOffsets(kafkaRDD)
      }

    })
    ssc.start()
    ssc.awaitTermination()
  }

}
