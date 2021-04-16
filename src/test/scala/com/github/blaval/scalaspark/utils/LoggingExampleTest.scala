package com.github.blaval.scalaspark.utils

import org.scalatest.{Matchers, WordSpec}

class LoggingExampleTest extends WordSpec with Matchers {
  "LoggingExample.logs" should {
    "logs a test with different level of logging" in {
      LoggingExample.logs("a test")
    }
  }
}
