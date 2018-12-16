package com.test.mina.server.kafka

import kafka.producer.Partitioner
import scala.math._
import kafka.utils.VerifiableProperties

class HashPartitioner extends Partitioner {
  def this(verifiableProperties: VerifiableProperties) { this }

  override def partition(key: Any, numPartitions: Int): Int = {

    if (key.isInstanceOf[Int]) {
      abs(key.toString().toInt) % numPartitions
    }

    key.hashCode() % numPartitions
  }

}
