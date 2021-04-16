package com.github.blaval.scalaspark.utils

import org.apache.logging.log4j.scala.Logging

object LoggingExample extends Logging {

  def logs(something: String): Unit = {
    logger.debug(s"Logs $something")
    logger.warn(s"Logs $something")
    logger.info(s"Logs $something")
    logger.error(s"Logs $something")
    logger.fatal(s"Logs $something")
  }
}
