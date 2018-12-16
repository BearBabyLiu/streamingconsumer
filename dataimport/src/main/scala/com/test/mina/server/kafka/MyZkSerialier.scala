package com.test.mina.server.kafka


import org.I0Itec.zkclient.exception.ZkMarshallingError
import org.I0Itec.zkclient.serialize.ZkSerializer
import java.nio.charset.Charset


class MyZkSerialier extends ZkSerializer {
  @throws[ZkMarshallingError]
  override def deserialize(bytes: Array[Byte]) = {
    if (bytes == null)
      null
    else
      new String(bytes, "UTF-8")
  }

  @throws[ZkMarshallingError]
  override def serialize(data: Any): Array[Byte] = data.asInstanceOf[String].getBytes("UTF-8")
}
