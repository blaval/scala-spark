package com.github.blaval.scalaspark.utils

import com.github.blaval.scalaspark.common.DbTable
import org.apache.spark.sql.{Dataset, Encoder, SaveMode}
import org.scalatest.Suite

trait HiveSpec extends SparkSpec {
  _: Suite =>

  def createDatabase(databaseName: String): Unit =
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $databaseName")

  def createTable[A](dataset: Dataset[A], table: DbTable): Unit = {
    createDatabase(table.database.name)
    dataset
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .saveAsTable(table.name)
  }

  def createEmptyTable[A: Encoder](table: DbTable): Unit = {
    import spark.implicits.localSeqToDatasetHolder
    createTable(Seq.empty[A].toDS(), table)
  }

}
