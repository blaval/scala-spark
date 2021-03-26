package com.github.blaval.scalaspark.scalaspark.utils

import org.apache.spark.sql.{Dataset, Encoder, SaveMode}
import org.scalatest.Suite

trait HiveSpec extends SparkSpec {
  _: Suite =>

  def createDatabase(databaseName: String): Unit =
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $databaseName")

  def createTable[A](dataset: Dataset[A], databaseName: String, tableName: String): Unit = {
    createDatabase(databaseName)
    dataset
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .saveAsTable(s"$databaseName.$tableName")
  }

  def createEmptyTable[A: Encoder](databaseName: String, tableName: String): Unit = {
    import spark.implicits.localSeqToDatasetHolder
    createTable(Seq.empty[A].toDS(), databaseName, tableName)
  }

}
