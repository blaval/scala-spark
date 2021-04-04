package com.github.blaval.scalaspark.dataframe

import com.github.blaval.scalaspark.dataframe.PivotComputation.PatientType._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class PivotComputation {

  private[dataframe] def pivotPatients(patients: DataFrame): DataFrame =
    patients.groupBy("provider_id").pivot("patient_type", validPatients).agg(sum("count"))

  private[dataframe] def unpivotPatients(patients: DataFrame): DataFrame =
    PivotComputation.unpivot(patients, "patient_type", "count", validPatients)

  def calculate(patients: DataFrame): DataFrame = {
    patients
      .transform(pivotPatients)
      .transform(unpivotPatients)
  }
}

object PivotComputation {

  object PatientType {
    val young = "young"
    val adult = "adult"
    val elder = "elder"

    val validPatients = Seq(young, adult, elder)
  }

  def unpivot(data: DataFrame, newColName: String, newMetricName: String, colsToUnpivot: Seq[String]): DataFrame = {
    val colsToKeep = (data.columns diff colsToUnpivot).map(col)
    val stack = colsToUnpivot.foldLeft((Option.empty[String], 0)) {
      case ((None, _), elt)        => Some(s"'$elt', $elt")       -> 1
      case ((Some(acc), idx), elt) => Some(s"$acc, '$elt', $elt") -> (idx + 1)
    }
    val colsToSelect =
      stack._1
        .map { s => colsToKeep :+ expr(s"stack(${stack._2}, $s) as ($newColName,$newMetricName)") }
        .getOrElse(colsToKeep)
    data.select(colsToSelect: _*)
  }
}
