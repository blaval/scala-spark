package com.github.blaval.scalaspark.scalaspark.runnable

import com.github.blaval.scalaspark.scalaspark.common.{Database, DbTable, Table}
import org.scalatest.{Matchers, WordSpec}

class DataFrameRunnableTest extends WordSpec with Matchers {
  "DataFrameRunnable" should {
    "have a correct name" in {
      val name = "DataFrameRunnable$"
      DataFrameRunnable.applicationName shouldBe s"com.github.blaval.scalaspark.scalaspark.runnable.$name"
    }
    "successfully accept all arguments" in {
      val inputArgs = Array(
        "--patientTable",
        "db.patients",
        "--physicianTable",
        "db.physicians",
        "--outputTable",
        "db.patients_with_physicians"
      )
      val db                    = Database("db")
      val dt: String => DbTable = table => DbTable(db, Table(table))
      DataFrameRunnable.readInputParameters(inputArgs) shouldBe DataFrameArgs(
        patientTable = dt("patients"),
        physicianTable = dt("physicians"),
        outputTable = dt("patients_with_physicians")
      )
    }
  }
}
