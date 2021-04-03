package com.github.blaval.scalaspark.utils

import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{date_format, udf}

import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, ZoneOffset}

object TimeUtils {

  object Format {
    val yyyyMMdd = "yyyyMMdd"
  }

  object DtTimeFormatter {
    val weekIdPattern: DateTimeFormatter = DateTimeFormatter.ofPattern(Format.yyyyMMdd)
  }

  object FromInt {

    def intToLocalDate(date: Int): LocalDate =
      LocalDate.parse(date.toString, DtTimeFormatter.weekIdPattern)

    def intToTimestamp(date: Int): Timestamp =
      Timestamp.valueOf(intToLocalDate(date).atStartOfDay())
  }

  object FromTimestamp {
    val tsToLocalDate: Timestamp => LocalDate = _.toInstant.atZone(ZoneOffset.UTC).toLocalDate

    val toMonday: UserDefinedFunction =
      udf((timestamp: Timestamp) => FromLocaDate.weekToInt(FromLocaDate.setDateToMonday(tsToLocalDate(timestamp))))
  }

  object FromLocaDate {
    val mondayDayValue = 1

    val setDateToDay: Int => LocalDate => LocalDate =
      dayValue => date => date.plusDays(dayValue - date.getDayOfWeek.getValue)

    val setDateToMonday: LocalDate => LocalDate = setDateToDay(mondayDayValue)
    val weekToString: LocalDate => String       = _.format((DateTimeFormatter.BASIC_ISO_DATE))
    val weekToInt: LocalDate => Int             = _.format((DateTimeFormatter.BASIC_ISO_DATE)).toInt
  }

  object FromDateColumn {
    val tsToDayCol: Column => Column = date_format(_, Format.yyyyMMdd)
  }
}
