package com.test.mina.server.auth

import com.test.mina.server.utils.ConfigManager
import redis.clients.jedis.Jedis
import collection.JavaConverters._


/**
  * 鑒權的基礎類
  */
trait AuthTrait {
  val authKey = ConfigManager.redisAuthKey
  // 获取所有已经认证的机器
  def getAllAuthMachine(jedis: Jedis): List[String] = {
    val machineNum = jedis.llen(authKey)
    if (machineNum != 0 ) jedis.lrange(authKey, 0, machineNum).asScala.toList
    else Nil
  }

}
