package com.github.blaval.scalaspark.scalaspark

import com.github.blaval.scalaspark.scalaspark.common.{Database, DbTable, Table}
import scopt.Read

package object runnable {

  implicit val dbTableReader: Read[DbTable] = Read.reads { input =>
    val parts = input.split("\\.")
    assert(parts.size == 2, "Input parameter must respect format: database.table")
    DbTable(Database(parts.head), Table(parts(1)))
  }
}
