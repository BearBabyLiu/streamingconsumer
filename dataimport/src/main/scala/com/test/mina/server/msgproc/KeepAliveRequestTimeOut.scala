package com.test.mina.server.msgproc

import com.test.mina.server.utils.LogSupport
import org.apache.mina.core.future.{CloseFuture, IoFuture, IoFutureListener}
import org.apache.mina.core.session.IoSession
import org.apache.mina.filter.keepalive.{KeepAliveFilter, KeepAliveRequestTimeoutHandler}


class KeepAliveRequestTimeOut extends KeepAliveRequestTimeoutHandler with LogSupport{
  @throws[Exception]
  override def keepAliveRequestTimedOut(filter: KeepAliveFilter, session: IoSession): Unit = {
    log.info("server端心跳包发送超时处理(即长时间没有发送（接受）心跳包)---关闭当前长连接")
    val closeFuture = session.close(true)
    closeFuture.addListener(new IoFutureListener[IoFuture]() {
      override def operationComplete(future: IoFuture): Unit = {
        if (future.isInstanceOf[CloseFuture]) {
          future.asInstanceOf[CloseFuture].setClosed()
          log.info("sessionClosed CloseFuture setClosed-->" + future.getSession.getId)
        }
      }
    })
  }
}
