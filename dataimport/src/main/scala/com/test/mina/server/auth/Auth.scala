package com.test.mina.server.auth

import com.test.mina.server.redis.RedisClient
import redis.clients.jedis.Jedis

object Auth extends AuthTrait{
  lazy val redisCli = RedisClient.getPool.getResource

  // 鉴权, 输入机器ID，返回鉴权是否成功
  def authMachine(machine: String): Boolean = {
    val allMachine = getAllAuthMachine(redisCli)
    allMachine.contains(machine)
  }
}
