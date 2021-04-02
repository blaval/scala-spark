package com.github.blaval.scalaspark.dataframe

import com.github.blaval.scalaspark.common.{Database, DbTable, Table, TableFunction}
import com.github.blaval.scalaspark.runnable
import com.github.blaval.scalaspark.runnable.DataFrameArgs
import com.github.blaval.scalaspark.utils.HiveSpec
import org.scalatest.WordSpec

class ExampleJobTest extends WordSpec with HiveSpec {

  "ExampleJob.run" should {
    "find physicians associated to patients" in {
      import spark.implicits._

      val patientTable           = "patients"
      val physicianTable         = "physicians"
      val physicianExcludedTable = "physicians_selected"
      val selectedPatientTable   = "patients_selected"
      val outputTable            = "patients_with_physicians"
      val args = ExampleJobTest.createArguments(
        patientTable,
        physicianTable,
        physicianExcludedTable,
        selectedPatientTable,
        outputTable
      )
      createTable(Seq("1", "2", "3", "4").toDF("patient_id"), args.patientTable)
      createTable(Seq("10" -> "2", "11" -> "3").toDF("physician_id", "patient_id"), args.physicianTable)
      createTable(Seq("10").toDF("physician_id"), args.physicianExcludedTable)
      createTable(Seq("3", "4").toDF("patient_id"), args.selectedPatientTable)
      new ExampleJob(spark, args, new TableFunction(spark)).run()

      val expected = Seq("3" -> "11", "4" -> null).toDF("patient_id", "physician_id")
      assertDataFrameEquals(spark.table(args.outputTable.name), expected)
    }
  }

}

object ExampleJobTest {

  def createArguments(
    patientTable: String,
    physicianTable: String,
    physicianExcludedTable: String,
    selectedPatientTable: String,
    outputTable: String
  ): DataFrameArgs = {
    val db                    = Database("db")
    val dt: String => DbTable = table => DbTable(db, Table(table))
    runnable.DataFrameArgs(
      patientTable = dt(patientTable),
      physicianTable = dt(physicianTable),
      physicianExcludedTable = dt(physicianExcludedTable),
      selectedPatientTable = dt(selectedPatientTable),
      outputTable = dt(outputTable)
    )
  }
}
