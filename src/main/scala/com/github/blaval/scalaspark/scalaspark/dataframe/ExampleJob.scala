package com.github.blaval.scalaspark.scalaspark.dataframe

import com.github.blaval.scalaspark.scalaspark.common.{Job, JoinType, TableFunction}
import com.github.blaval.scalaspark.scalaspark.runnable.DataFrameArgs
import org.apache.spark.sql.{DataFrame, SparkSession}

class ExampleJob(spark: SparkSession, argument: DataFrameArgs, tableFunction: TableFunction) extends Job {
  implicit val sparkSession: SparkSession = spark
  import spark.implicits.StringToColumn

  override def run(): Unit = {
    val patients   = spark.table(argument.patientTable)
    val physicians = spark.table(argument.physicianTable)

    val patientsWithPhysicianDetails = ExampleJob.joinPatientsAndPhysicians(patients, physicians)

    tableFunction.overwrite(patientsWithPhysicianDetails.repartition(1, $"patient_id"), argument.outputTable)
  }
}

object ExampleJob {

  def joinPatientsAndPhysicians(patients: DataFrame, physicians: DataFrame): DataFrame = {
    patients.join(physicians, Seq("patient_id"), JoinType.leftOuter)
  }

}
