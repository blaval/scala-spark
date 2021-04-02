package com.github.blaval.scalaspark.common

import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

class TableFunction(spark: SparkSession) {

  def overwrite(data: Dataset[_], table: DbTable): Unit = {
    spark.sql(s"DROP TABLE IF EXISTS ${table.name}")
    data.write
      .mode(SaveMode.Overwrite)
      .format(Format.parquet)
      .saveAsTable(table.name)
  }
}
