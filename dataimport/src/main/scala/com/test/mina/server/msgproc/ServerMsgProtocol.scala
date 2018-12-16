package com.test.mina.server.msgproc

import java.io.IOException
import java.net.InetSocketAddress
import java.nio.charset.Charset

import com.test.mina.server.utils.{ConfigManager, LogSupport}
import org.apache.mina.core.session.IdleStatus
import org.apache.mina.filter.codec.ProtocolCodecFilter
import org.apache.mina.filter.codec.textline.TextLineCodecFactory
import org.apache.mina.filter.keepalive.{KeepAliveFilter, KeepAliveMessageFactory, KeepAliveRequestTimeoutHandler}
import org.apache.mina.filter.logging.{LogLevel, LoggingFilter}
import org.apache.mina.transport.socket.SocketAcceptor
import org.apache.mina.transport.socket.nio.NioSocketAcceptor

object ServerMsgProtocol extends LogSupport {
  private val IDELTIMEOUT = ConfigManager.idelTimeOut
  //15秒发送一次心跳包
  private val HEARTBEATRATE = ConfigManager.heartbeatRate
  private val port: Int = ConfigManager.serverPort
  private val codeC: String = ConfigManager.serverCodec
  private val cacheSize: Int = ConfigManager.cacheSize
  private var acceptor: SocketAcceptor = _

  def getAcceptor: SocketAcceptor = {
    if (null == acceptor) { // 创建非堵塞的server端的Socket连接
      acceptor = new NioSocketAcceptor()
    }
    acceptor
  }

  def serverStart: Boolean = {
    val filterChain = getAcceptor.getFilterChain
    // 加入编码过滤器 处理乱码、编码问题
    log.info("add codec filter.")
    filterChain.addLast("codec", new ProtocolCodecFilter(
      new TextLineCodecFactory(Charset.forName(codeC),
        "\r\n", "\r\n")))

    // 加入鉴权认证过滤器
    log.info("add auth module......")
    filterChain.addLast("auth", new AuthFilter())

    // 解析得到mina框架日志级别
    val logLevel = ConfigManager.minaLogLevel.toLowerCase match {
      case "info" => LogLevel.INFO
      case "debug" => LogLevel.DEBUG
      case "warn" => LogLevel.WARN
      case "error" => LogLevel.ERROR
      case "none" => LogLevel.NONE
      case _ =>
        log.debug("not support log level, will use none")
        LogLevel.NONE
    }

    val loggingFilter = new LoggingFilter
    loggingFilter.setMessageReceivedLogLevel(logLevel)
    loggingFilter.setMessageSentLogLevel(logLevel)
    // 加入日志过滤器
    log.info("add logger filter.")
    filterChain.addLast("logger", loggingFilter)
    // 设置核心消息业务处理器
    getAcceptor.setHandler(new ServerMsg)
    // 心跳包处理逻辑
    log.info("add heartBeat filter.")
    val heartBeatFactory: KeepAliveMessageFactory = new KeepAliveMessage
    val heartBeatHandler: KeepAliveRequestTimeoutHandler = new KeepAliveRequestTimeOut
    val heartBeat: KeepAliveFilter = new KeepAliveFilter(heartBeatFactory, IdleStatus.BOTH_IDLE, heartBeatHandler)
    // 是否回发
    heartBeat.setForwardEvent(false)
    // 发送频率
    heartBeat.setRequestInterval(HEARTBEATRATE)
    // 加入心跳过滤器
    getAcceptor.getFilterChain.addLast("heartbeat", heartBeat)



    // 接收缓冲区设置为1M
    getAcceptor.getSessionConfig.setReceiveBufferSize(cacheSize)

    //    getAcceptor.getSessionConfig.setBothIdleTime(30)
    getAcceptor.getSessionConfig.setKeepAlive(true)
    // 设置session配置，30秒内无操作进入空暇状态
    getAcceptor.getSessionConfig.setIdleTime(IdleStatus.BOTH_IDLE, IDELTIMEOUT)
    try { // 绑定端口3456
      getAcceptor.bind(new InetSocketAddress(port))
      log.info(s"bind with port: $port success")
      true
    } catch {
      case e: IOException =>
        log.error(s"start server failed. try again...... ${e.getStackTraceString}")
        e.printStackTrace()
        false
    }

  }
}
