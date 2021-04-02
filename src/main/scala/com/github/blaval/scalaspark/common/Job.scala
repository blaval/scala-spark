package com.github.blaval.scalaspark.common

abstract class Job extends java.io.Serializable {
  def run(): Unit
}
