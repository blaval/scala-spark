package com.github.blaval.scalaspark.scalaspark.dataframe

import com.github.blaval.scalaspark.scalaspark.common._
import com.github.blaval.scalaspark.scalaspark.runnable.DataFrameArgs
import com.github.blaval.scalaspark.scalaspark.utils.HiveSpec
import org.scalatest.WordSpec

class ExampleJobTest extends WordSpec with HiveSpec {

  "ExampleJob.run" should {
    "find physicians associated to patients" in {
      import spark.implicits._

      val patientTable   = "patients"
      val physicianTable = "physicians"
      val outputTable    = "patients_with_physicians"
      val args           = ExampleJobTest.createArguments(patientTable, physicianTable, outputTable)
      createTable(Seq("1", "2").toDF("patient_id"), args.patientTable)
      createTable(Seq("10" -> "2", "11" -> "3").toDF("physician_id", "patient_id"), args.physicianTable)
      new ExampleJob(spark, args, new TableFunction(spark)).run()

      val expectedPatients = Seq("1" -> null, "2" -> "10").toDF("patient_id", "physician_id")
      assertDataFrameEquals(spark.table(args.outputTable.name), expectedPatients)
    }
  }

}

object ExampleJobTest {

  def createArguments(patientTable: String, physicianTable: String, outputTable: String): DataFrameArgs = {
    val db                    = Database("db")
    val dt: String => DbTable = table => DbTable(db, Table(table))
    DataFrameArgs(patientTable = dt(patientTable), physicianTable = dt(physicianTable), outputTable = dt(outputTable))
  }
}
