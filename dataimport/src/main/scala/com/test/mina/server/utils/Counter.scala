package com.test.mina.server.utils

object Counter {
  var dataTopicCounter = 0
  var statusTopicCounter = 0
  var aGpsTopicCounter = 0
  var stateOneTopicCounter = 0
  val partitions = ConfigManager.kafkaPartitions

  def getDataTopicCounter: Int = {
    if (dataTopicCounter < partitions) {
      dataTopicCounter += 1
      dataTopicCounter
    }
    else {
      dataTopicCounter = 0
      dataTopicCounter
    }
  }

  def getStatusTopicCounter: Int = {
    if (statusTopicCounter < partitions) {
      statusTopicCounter += 1
      statusTopicCounter
    }
    else {
      statusTopicCounter = 0
      statusTopicCounter
    }
  }

  def getAgpsTopicCounter: Int = {
    if (aGpsTopicCounter < partitions) {
      aGpsTopicCounter += 1
      aGpsTopicCounter
    }
    else {
      aGpsTopicCounter = 0
      aGpsTopicCounter
    }
  }

  def getStateOneTopicCounter: Int = {
    if (stateOneTopicCounter < partitions) {
      stateOneTopicCounter += 1
      stateOneTopicCounter
    }
    else {
      stateOneTopicCounter = 0
      stateOneTopicCounter
    }
  }
}
