package com.github.blaval.scalaspark.scalaspark.common

import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

class TableFunction(spark: SparkSession) {

  def overwrite(data: Dataset[_], table: String): Unit = {
    spark.sql(s"DROP TABLE IF EXISTS $table")
    data.write
      .mode(SaveMode.Overwrite)
      .format(Format.parquet)
      .saveAsTable(table)
  }
}
