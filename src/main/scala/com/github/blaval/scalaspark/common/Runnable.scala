package com.github.blaval.scalaspark.common

import org.apache.spark.sql.SparkSession

trait RunnableArgs {
  def checkpointDirectory: Option[String] = None
}

trait Runnable[A <: RunnableArgs] {
  def applicationName: String
  def readInputParameters(args: Array[String]): A
  def createJob(appContext: AppContext, spark: SparkSession, argument: A): Job

  def main(args: Array[String]): Unit = {
    val appContext = new AppContext()
    val arguments  = readInputParameters(args)
    val spark      = appContext.setupAppContext(applicationName, arguments)

    try {
      createJob(appContext, spark, arguments).run()
    } catch {
      case e: Exception => throw e
    } finally {
      appContext.tearDownContext(spark, arguments)
    }
  }
}
