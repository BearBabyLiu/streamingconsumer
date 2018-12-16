package com.test.mina.server.redis

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{Jedis, JedisPool}
import com.test.mina.server.utils.ConfigManager

/**
  * Redis 连接池
  */
object RedisClient {
  private val redisHost: String = ConfigManager.redisHost
  private val redisPort: Int = ConfigManager.redisPort
  private val redisTimeOut: Int = ConfigManager.redisTimeout // ms
  private val maxTotal = ConfigManager.redisMaxTotal
  private val maxIdle = ConfigManager.redisMaxIdel
  private val minIdle = ConfigManager.redisMinIdel
  private val pwd = ConfigManager.redisPwd
  private val database =  ConfigManager.redisDataBase

  @transient private var pool: JedisPool = _
  makePool(redisHost, redisPort, redisTimeOut, maxTotal, maxIdle, minIdle, database)

  def makePool(redisHost: String, redisPort: Int, redisTimeout: Int,
               maxTotal: Int, maxIdle: Int, minIdle: Int, dataBase: Int): Unit = {
    makePool(redisHost, redisPort, redisTimeout, maxTotal, maxIdle,
      minIdle, true, false, 10000, dataBase)
  }

  def makePool(redisHost: String, redisPort: Int, redisTimeout: Int,
               maxTotal: Int, maxIdle: Int, minIdle: Int, testOnBorrow: Boolean,
               testOnReturn: Boolean, maxWaitMillis: Long, dataBase: Int = 0): Unit = {
    if (pool == null) {
      val poolConfig = new GenericObjectPoolConfig()
      poolConfig.setMaxTotal(maxTotal)
      poolConfig.setMaxIdle(maxIdle)
      poolConfig.setMinIdle(minIdle)
      poolConfig.setTestOnBorrow(testOnBorrow)
      poolConfig.setTestOnReturn(testOnReturn)
      poolConfig.setMaxWaitMillis(maxWaitMillis)
      pool = new JedisPool(poolConfig, redisHost, redisPort, redisTimeout, pwd)

      val hook = new Thread {
        override def run: Unit = pool.destroy()
      }
      sys.addShutdownHook(hook.run)
    }
  }

  def getPool: JedisPool = {
    assert(pool != null)
    pool
  }


  def main(args: Array[String]) = {
    val jedis: Jedis = getPool.getResource
    val value = jedis.set("name1", "test")
    println(s"value is $value")
    jedis.close()
  }
}
