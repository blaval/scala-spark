package com.github.blaval.scalaspark.dataframe

import com.github.blaval.scalaspark.model.Period
import org.apache.spark.sql.functions.datediff
import org.apache.spark.sql.{DataFrame, SparkSession}

final class TimeComputation(spark: SparkSession, period: Period) {
  import spark.implicits.StringToColumn

  private[dataframe] def selectWithinPeriod(df: DataFrame): DataFrame =
    df.filter($"service_date" >= period.start && $"service_date" <= period.end)

  private[dataframe] def serviceDuration(df: DataFrame): DataFrame =
    df.withColumn("service_duration", datediff($"service_date_end", $"service_date"))

  def calculate(patients: DataFrame): DataFrame = {
    patients
      .transform(selectWithinPeriod)
      .transform(serviceDuration)
  }
}
