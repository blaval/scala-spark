package com.github.blaval.scalaspark.scalaspark.runnable

import com.github.blaval.scalaspark.scalaspark.common.{AppContext, Job, Runnable, TableFunction}
import com.github.blaval.scalaspark.scalaspark.dataframe.ExampleJob
import org.apache.spark.sql.SparkSession
import scopt.OptionParser

class DataFrameRunnable extends Runnable[DataFrameArgs] {
  override def applicationName: String = this.getClass.getName

  override def readInputParameters(args: Array[String]): DataFrameArgs = {
    val parser = new OptionParser[DataFrameArgs]("DataFrame") {
      opt[String]("patientTable")
        .text("patient table name, i.e db.patients")
        .action((value, param) => param.copy(patientTable = value))
        .required()
      opt[String]("physicianTable")
        .text("physician table name, i.e db.physicians")
        .action((value, param) => param.copy(physicianTable = value))
        .required()
      opt[String]("outputTable")
        .text("output table name, i.e db.patients_with_physicians")
        .action((value, param) => param.copy(outputTable = value))
        .required()
    }

    parser
      .parse(args, DataFrameArgs())
      .getOrElse(throw new IllegalArgumentException(s"Invalid input parameters ${args.mkString(", ")}"))
  }

  override def createJob(appContext: AppContext, spark: SparkSession, arguments: DataFrameArgs): Job = {
    new ExampleJob(spark, arguments, new TableFunction(spark))
  }
}

case class DataFrameArgs(patientTable: String = "", physicianTable: String = "", outputTable: String = "")
