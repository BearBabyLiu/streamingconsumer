package scala.com.bigdata

import org.slf4j.LoggerFactory

/**
  * Created by Administrator on 2018/11/20.
  */
trait LogSupport {
  private val log = LoggerFactory.getLogger(this.getClass)

  protected def logDebug(msg: => String): Unit ={
    if(log.isDebugEnabled) {
      log.debug(msg)
    }
  }

  protected def logInfo(msg: => String): Unit ={
    if(log.isInfoEnabled) {
      log.info(msg)
    }
  }

  protected def logError(msg: => String, exception: Throwable = null): Unit ={
    if(log.isErrorEnabled) {
      log.error(msg, exception)
    }
  }
}
