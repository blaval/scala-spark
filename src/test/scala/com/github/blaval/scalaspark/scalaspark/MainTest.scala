package com.github.blaval.scalaspark.scalaspark

import com.github.blaval.scalaspark.scalaspark.utils.HiveSpec
import org.scalatest.{Matchers, WordSpec}

class MainTest extends WordSpec with Matchers with HiveSpec {
  import spark.implicits._

  "MainTest" should {
    "succeed" in {
      val db    = "db_test"
      val table = "table"
      createTable(Seq(1).toDF, db, table)
      val result   = spark.table(s"$db.$table")
      val expected = Seq(1).toDF
      assertDataFrameDataEquals(expected, result)
    }
  }
}
