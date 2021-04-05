package com.github.blaval.scalaspark.runnable

import com.github.blaval.scalaspark.common.{AppContext, DbTable, Job, Runnable, RunnableArgs, TableFunction}
import com.github.blaval.scalaspark.dataset.ExampleJob
import org.apache.spark.sql.SparkSession
import scopt.OptionParser

object DatasetRunnable extends Runnable[DatasetArgs] {
  override def applicationName: String = this.getClass.getName

  override def readInputParameters(args: Array[String]): DatasetArgs = {
    val parser = new OptionParser[DatasetArgs]("Dataset") {
      opt[DbTable]("patientTable")
        .text("patient table name, i.e db.patients")
        .action((value, param) => param.copy(patientTable = value))
        .required()
      opt[DbTable]("outputTable")
        .text("output table name, i.e db.patients_selected")
        .action((value, param) => param.copy(outputTable = value))
        .required()
    }

    parser
      .parse(args, DatasetArgs())
      .getOrElse(throw new IllegalArgumentException(s"Invalid input parameters ${args.mkString(", ")}"))
  }

  override def createJob(appContext: AppContext, spark: SparkSession, arguments: DatasetArgs): Job = {
    new ExampleJob(spark, arguments, new TableFunction(spark))
  }
}

case class DatasetArgs(patientTable: DbTable = DbTable(), outputTable: DbTable = DbTable()) extends RunnableArgs
