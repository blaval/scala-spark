package com.github.blaval.scalaspark.model

import java.sql.Date
import java.time.LocalDate

final case class Period(start: Date, end: Date) {
  require(!start.toLocalDate.isAfter(end.toLocalDate), "Start must be set before end to define a period")
}

object Period {
  def apply(start: LocalDate, end: LocalDate): Period = Period(Date.valueOf(start), Date.valueOf(end))
}
