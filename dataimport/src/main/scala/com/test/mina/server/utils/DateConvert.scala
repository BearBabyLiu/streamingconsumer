package com.test.mina.server.utils

import java.text.SimpleDateFormat
import java.util.Date

trait DateConvert {
  def getNowDate: String = {
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    dateFormat.format(new Date())
  }
}
