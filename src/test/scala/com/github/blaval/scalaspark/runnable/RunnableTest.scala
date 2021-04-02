package com.github.blaval.scalaspark.runnable

import com.github.blaval.scalaspark.common.{Database, DbTable, Table}
import org.scalatest.{Matchers, WordSpec}

class RunnableTest extends WordSpec with Matchers {
  "Runnable package object" should {
    "be able to parse a DbTable string" in {
      dbTableReader.reads("db.table") shouldBe DbTable(Database("db"), Table("table"))
    }
    "throw an exception if DbTable input parameters does not respect the correct format" in {
      the[IllegalArgumentException] thrownBy {
        dbTableReader.reads("dbtable")
      } should have message "requirement failed: Input parameter must respect format: database.table"
    }
  }
}
