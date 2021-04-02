package com.github.blaval.scalaspark.scalaspark.dataframe

import com.github.blaval.scalaspark.scalaspark.common.{Job, JoinType, TableFunction}
import com.github.blaval.scalaspark.scalaspark.runnable.DataFrameArgs
import org.apache.spark.sql.{DataFrame, SparkSession}

class ExampleJob(spark: SparkSession, arguments: DataFrameArgs, tableFunction: TableFunction) extends Job {
  implicit val sparkSession: SparkSession = spark
  import spark.implicits.StringToColumn

  override def run(): Unit = {
    import arguments._
    val patients           = spark.table(patientTable.name)
    val physicians         = spark.table(physicianTable.name)
    val physiciansExcluded = spark.table(physicianExcludedTable.name)
    val selectedPatients   = spark.table(selectedPatientTable.name)

    val patientsWithPhysicianDetails =
      ExampleJob.joinPatientsAndPhysicians(patients, physicians, physiciansExcluded, selectedPatients)

    tableFunction.overwrite(patientsWithPhysicianDetails.repartition(1, $"patient_id"), outputTable)
  }
}

object ExampleJob {

  def joinPatientsAndPhysicians(
    patients: DataFrame,
    physicians: DataFrame,
    physiciansExcluded: DataFrame,
    selectedPatients: DataFrame
  ): DataFrame = {
    val physiciansFiltered = physicians.join(physiciansExcluded, Seq("physician_id"), JoinType.leftAnti)
    patients
      .join(selectedPatients, Seq("patient_id"), JoinType.leftSemi)
      .join(physiciansFiltered, Seq("patient_id"), JoinType.leftOuter)
  }

}
