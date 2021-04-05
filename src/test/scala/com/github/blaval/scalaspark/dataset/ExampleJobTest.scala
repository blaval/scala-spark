package com.github.blaval.scalaspark.dataset

import com.github.blaval.scalaspark.common.{Database, DbTable, Table, TableFunction}
import com.github.blaval.scalaspark.runnable
import com.github.blaval.scalaspark.runnable.DatasetArgs
import com.github.blaval.scalaspark.utils.HiveSpec
import org.scalatest.{Matchers, WordSpec}

class ExampleJobTest extends WordSpec with HiveSpec with Matchers {

  "ExampleJob.run" should {
    "return aggregated patient having at least 1 XO1034 prescription" in {
      import spark.implicits._

      val patientTable = "patients"
      val outputTable  = "patients_selected"
      val args         = ExampleJobTest.createArguments(patientTable, outputTable)
      createTable(
        Seq(
          Patient(1, Seq(Prescription("PO1000", 2.36), Prescription("PA1000", 10.64), Prescription("AA1050", 6.85))),
          Patient(2, Seq(Prescription("LO1010", 5.35), Prescription("PA1000", 10.64), Prescription("QA1080", 4.87))),
          Patient(1, Seq(Prescription("PO1000", 2.36), Prescription(ExampleJob.prescriptionXO1034, 506.30)))
        ).toDS,
        args.patientTable
      )

      new ExampleJob(spark, args, new TableFunction(spark)).run()
      val result = spark.table(args.outputTable.name).as[Patient].collect().toList
      result.size shouldBe 1

      val expected = Seq(
        Patient(
          1,
          Seq(
            Prescription("PO1000", 2.36),
            Prescription("PA1000", 10.64),
            Prescription("AA1050", 6.85),
            Prescription("PO1000", 2.36),
            Prescription(ExampleJob.prescriptionXO1034, 506.30)
          )
        )
      )
      result should contain theSameElementsAs expected
    }
  }

}

object ExampleJobTest {

  def createArguments(patientTable: String, outputTable: String): DatasetArgs = {
    val db                    = Database("db")
    val dt: String => DbTable = table => DbTable(db, Table(table))
    runnable.DatasetArgs(patientTable = dt(patientTable), outputTable = dt(outputTable))
  }
}
