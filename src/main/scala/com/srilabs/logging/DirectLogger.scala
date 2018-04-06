package com.srilabs.logging

import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

object DirectLogger extends  Serializable {

  @transient
  lazy val logger: Logger =
    Logger(LoggerFactory.getLogger(getClass.getName))

  // def getLogger: Logger = logger

  def elasticSearchLogAppender = ???
  def solrLogAppender = ???

}