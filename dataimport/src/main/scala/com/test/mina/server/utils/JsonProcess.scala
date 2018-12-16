package com.test.mina.server.utils

import java.util

import net.minidev.json.JSONObject
import net.minidev.json.parser.JSONParser
import org.json4s.NoTypeHints
import org.json4s.native.Serialization
import org.json4s.native.Serialization.{read, write}
import scala.reflect. Manifest

import org.json4s._
import org.json4s.jackson.JsonMethods._

trait JsonProcess {

  def parseJson(str: String): JSONObject = {
    val jsonParser = new JSONParser()
    jsonParser.parse(str).asInstanceOf[JSONObject]
  }

  def json2CaseClass[T: Manifest](str: String): T = {
    implicit val formats = Serialization.formats(NoTypeHints)
    read[T](str)
  }

  def caseClass2Json(caseClass: AnyRef): String = {
    implicit val formats = Serialization.formats(NoTypeHints)
    write(caseClass)
  }

  def json2CaseClass2[T: Manifest](str: String): T = {
    implicit val formats = DefaultFormats
    parse(str).extract[T]
  }

}
