package com.github.blaval.scalaspark.runnable

import com.github.blaval.scalaspark.common.{Database, DbTable, Table}
import org.scalatest.{Matchers, WordSpec}

class DataFrameRunnableTest extends WordSpec with Matchers {
  "DataFrameRunnable" should {
    "have a correct name" in {
      val name = "DataFrameRunnable$"
      DataFrameRunnable.applicationName shouldBe s"com.github.blaval.scalaspark.runnable.$name"
    }
    "successfully accept all arguments" in {
      val inputArgs = Array(
        "--patientTable",
        "db.patients",
        "--physicianTable",
        "db.physicians",
        "--physicianExcludedTable",
        "db.physicians_excluded",
        "--selectedPatientTable",
        "db.patients_selected",
        "--outputTable",
        "db.patients_with_physicians"
      )
      val db                    = Database("db")
      val dt: String => DbTable = table => DbTable(db, Table(table))
      DataFrameRunnable.readInputParameters(inputArgs) shouldBe DataFrameArgs(
        patientTable = dt("patients"),
        physicianTable = dt("physicians"),
        physicianExcludedTable = dt("physicians_excluded"),
        selectedPatientTable = dt("patients_selected"),
        outputTable = dt("patients_with_physicians")
      )
    }
  }
}
