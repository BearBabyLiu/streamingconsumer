package com.test.mina.server.msgproc

import com.test.mina.server.utils.{DateConvert, JsonProcess, LogSupport}
import org.apache.mina.core.session.IoSession
import org.apache.mina.filter.keepalive.KeepAliveMessageFactory
import org.json4s.NoTypeHints
import org.json4s.native.Serialization
import org.json4s.native.Serialization.write
import com.test.mina.server.utils.FuncDefine
import com.test.mina.server.utils.HeartBeatFrame

/**
  * @see 发送心跳包的内容
  *      getResponse()---->isResponse();获取数据推断心跳事件（目的是推断是否触发心跳超时异常）
  *      isRequest()----->getRequest(); 写回数据是心跳事件触发的数据（目的写回给server（client）心跳包）
  */


class KeepAliveMessage extends KeepAliveMessageFactory with LogSupport
  with JsonProcess with DateConvert {

  /**
    * @see 向客户端定时发送的心跳包
    */
  override def getRequest(session: IoSession): Object = {
    val existSession = session.getAttribute("heart")
    if (null == existSession) null
    else {
      val heartBeat = json2CaseClass[HeartBeatFrame](existSession.asInstanceOf[String])
      heartBeat.time = getNowDate
      implicit val formats = Serialization.formats(NoTypeHints)
      write(heartBeat)
    }
  }

  /**
    * @see 接受到的client数据包后需要回复响应的心跳包
    */
  override def getResponse(session: IoSession, message: Object): Object = {
    log.info(s"client[ ${session.getRemoteAddress}/${session.getId}] 心跳: " + message)
    val msg = message.asInstanceOf[String]
    // 将数据转换为case class
    val heartBeat = json2CaseClass[HeartBeatFrame](msg)
    heartBeat.func = FuncDefine.HEARTBEATRESPONSE
    heartBeat.time = getNowDate

    implicit val formats = Serialization.formats(NoTypeHints)
    val data = write(heartBeat)
    session.setAttribute("heart", data)
    data
  }

  /**
    * @see 推断是否是client发送来的的心跳包此推断影响
    *      KeepAliveRequestTimeoutHandler实现类推断是否心跳包发送超时
    */
  override def isRequest(session: IoSession, message: Object): Boolean = {
    val msg = message.asInstanceOf[String]
    if (!msg.contains("func")) {
      log.error(s"isRequest: this is not a valid command: $msg")
      session.closeNow
      return false
    }
    val strObj = parseJson(msg)
    val funcCode = strObj.get("func").toString.toInt
    if (funcCode == FuncDefine.HEARTBEATREQUEST) true else false
  }

  /**
    * @see 推断发送信息是否是心跳数据包此推断影响
    *      KeepAliveRequestTimeoutHandler实现类 推断是否心跳包发送超时
    */
  override def isResponse(session: IoSession, message: Object): Boolean = {
    val msg = message.asInstanceOf[String]
    if (!msg.contains("func")) {
      log.error(s"isResponse: this is not a valid command: $msg")
      session.closeNow
      return false
    }
    val strObj = parseJson(msg)
    val funcCode = strObj.get("func").toString.toInt
    if (funcCode == FuncDefine.HEARTBEATRESPONSE) {
      return true
    }
    false
  }
}

