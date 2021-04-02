package com.github.blaval.scalaspark.scalaspark.common

case class Database(name: String = "")
case class Table(name: String = "")

case class DbTable(database: Database = Database(), table: Table = Table()) {
  lazy val name = s"${database.name}.${table.name}"
}
