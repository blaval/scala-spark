package com.github.blaval.scalaspark.dataframe

import org.apache.spark.sql.functions.datediff
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.Date
import java.time.LocalDate

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

final case class Period(start: Date, end: Date)

object Period {
  def apply(start: LocalDate, end: LocalDate): Period = Period(Date.valueOf(start), Date.valueOf(end))
}
