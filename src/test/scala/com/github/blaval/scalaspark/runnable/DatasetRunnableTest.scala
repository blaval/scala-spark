package com.github.blaval.scalaspark.runnable

import com.github.blaval.scalaspark.common.{Database, DbTable, Table}
import org.scalatest.{Matchers, WordSpec}

class DatasetRunnableTest extends WordSpec with Matchers {
  "DatasetRunnable" should {
    "have a correct name" in {
      val name = "DatasetRunnable$"
      DatasetRunnable.applicationName shouldBe s"com.github.blaval.scalaspark.runnable.$name"
    }
    "successfully accept all arguments" in {
      val inputArgs             = Array("--patientTable", "db.patients", "--outputTable", "db.patients_selected")
      val db                    = Database("db")
      val dt: String => DbTable = table => DbTable(db, Table(table))
      DatasetRunnable.readInputParameters(inputArgs) shouldBe DatasetArgs(
        patientTable = dt("patients"),
        outputTable = dt("patients_selected")
      )
    }
  }
}
