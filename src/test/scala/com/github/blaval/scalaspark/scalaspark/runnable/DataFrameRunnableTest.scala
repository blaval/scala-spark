package com.github.blaval.scalaspark.scalaspark.runnable

import org.scalatest.{Matchers, WordSpec}

class DataFrameRunnableTest extends WordSpec with Matchers {
  "DataFrameRunnable.run" should {
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
      DataFrameRunnable.readInputParameters(inputArgs) shouldBe DataFrameArgs(
        patientTable = "db.patients",
        physicianTable = "db.physicians",
        outputTable = "db.patients_with_physicians"
      )
    }
  }
}
