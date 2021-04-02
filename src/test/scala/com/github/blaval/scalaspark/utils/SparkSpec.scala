package com.github.blaval.scalaspark.utils

import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset}
import org.scalatest.Suite

trait SparkSpec extends DatasetSuiteBase {
  _: Suite =>

  override def conf: SparkConf =
    super.conf.set("spark.sql.shuffle.partitions", "1")

  def assertDatasetNoOrderEquals[A](expected: Dataset[A], result: Dataset[A]): Unit =
    assertDataFrameNoOrderEquals(expected.toDF(), result.toDF())

  override def assertDataFrameNoOrderEquals(expected: DataFrame, result: DataFrame): Unit = {
    val sortDf: DataFrame => DataFrame = df => df.sort(df.columns.map(col): _*)
    assertDataFrameEquals(sortDf(expected), sortDf(result))
  }

}
