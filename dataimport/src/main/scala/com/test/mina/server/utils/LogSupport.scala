package com.test.mina.server.utils

import org.apache.log4j.PropertyConfigurator
import org.slf4j.LoggerFactory


trait LogSupport {
  protected val log = LoggerFactory.getLogger(this.getClass)
}
