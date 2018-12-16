package com.test.mina.server.utils

import java.io.File

import com.typesafe.config.ConfigFactory

object ConfigManager {

  private val directory = new File(".")
  private val filePath: String = directory.getAbsolutePath //设定为上级文件夹 获取绝对路径
  private val usrDefinedPath = filePath + System.getProperty("file.separator") + "conf"
  println(s"usrDefinedPath is $usrDefinedPath")
  private val defaultConfig = ConfigFactory.load("dataimport.conf")

  // 读取用户文件夹下所有的以.conf结尾的文件
  private val usrDefPath = new File(usrDefinedPath)

  val config = if (usrDefPath.exists && usrDefPath.listFiles.nonEmpty) {
    val usrDefConf = usrDefPath.listFiles.filter(f => f.getName.endsWith(".conf")).map(file => {
      ConfigFactory.load(file.getAbsolutePath)
    }).reduce((x, y) => x.withFallback(y))
    // 最终的配置项
    usrDefConf.withFallback(defaultConfig)
  } else defaultConfig


  lazy val serverPort = config.getInt("server.serverport")   // 对外服务端口
  lazy val idelTimeOut = config.getInt("server.ideltimeout") // 客户端无活动超时时间
  lazy val heartbeatRate = config.getInt("server.heartbeatrate")  // 服务端心跳发送频率
  lazy val serverCodec = config.getString("server.serverparsecode") // 服务器对数据包的编码方式
  lazy val cacheSize = config.getInt("server.serverCacheSize")  // 缓冲大小
  lazy val minaLogLevel = config.getString("server.serverloglevel") // mina日志级别

  // redis 相关配置
  lazy val redisHost = config.getString("redis.host")
  lazy val redisPort = config.getInt("redis.port")
  lazy val redisTimeout = config.getInt("redis.timeout")
  lazy val redisMaxTotal = config.getInt("redis.maxTotal")
  lazy val redisMaxIdel = config.getInt("redis.maxIdle")
  lazy val redisMinIdel = config.getInt("redis.minIdle")
  lazy val redisPwd = config.getString("redis.pwd")
  lazy val redisDataBase = config.getInt("redis.database")

  lazy val redisAuthKey = config.getString("redis.authKey")
  lazy val redisUnAuthKey = config.getString("redis.unAuthKey")

  // kafka 相关配置
  lazy val kafkaBroker = config.getString("kafka.broker")
  lazy val zooKeeperServer = config.getString("kafka.zkServer")
  lazy val kafkaSessionTimeout = config.getInt("kafka.sessionTimeout")
  lazy val kafkaConnectTimeout = config.getInt("kafka.connectTimeout")
  lazy val kafkaSecureKafkaCluster = config.getBoolean("kafka.isSecureKafkaCluster")
  lazy val kafkaPartitions = config.getInt("kafka.partitions")
  lazy val kafkaReplication = config.getInt("kafka.replication")
  lazy val kafkaRetryTimes = config.getInt("kafka.retryTimes")
  lazy val kafkaBatchSize = config.getLong("kafka.batchSize")
  lazy val kafkaLinerMs = config.getInt("kafka.lingerMs")
  lazy val kafkaBufferMemory = config.getLong("kafka.bufferMemory")

  lazy val hcDataTopic = config.getString("topic.huachenDataTopic")
  lazy val hcStatusTopic = config.getString("topic.huachenStatusTopic")
  lazy val hcGpsTopic = config.getString("topic.huachengGpsTopic")
  lazy val stateOneTopic = config.getString("topic.oldStateDataTopic")

  lazy val topicsList = List(hcDataTopic, hcStatusTopic, hcGpsTopic, stateOneTopic)


}
