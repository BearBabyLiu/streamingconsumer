package com.test.mina.server

import com.test.mina.server.kafka.HuaChengProducer
import com.test.mina.server.msgproc.ServerMsgProtocol
import com.test.mina.server.utils.{ConfigManager, LogSupport}


object Boot extends LogSupport {
  def main(args: Array[String]): Unit = {
    log.info("program is starting..................")
    // 1、连接kafka检查相关topic是否已经创建
    val producerObj = new HuaChengProducer()
    log.info("step 1: try to create topic in kafka.")
    ConfigManager.topicsList.foreach(topic => {
      producerObj.createTopic(topic, 3, ConfigManager.kafkaPartitions,
        ConfigManager.kafkaReplication)
    })
    log.info("step 2: start socket server.....")
    ServerMsgProtocol.serverStart
  }
}
