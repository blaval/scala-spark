package com.github.blaval.scalaspark.utils

import org.scalatest.{Matchers, WordSpec}

import java.sql.Timestamp
import java.time.LocalDate
import java.time.format.DateTimeFormatter

class TimeUtilsTest extends WordSpec with Matchers {
  "TimeUtils.Format.yyyyMMdd" should {
    "be yyyyMMdd" in {
      TimeUtils.Format.yyyyMMdd shouldBe "yyyyMMdd"
    }
  }

  "TimeUtils.DtTimeFormatter.weekIdPattern" should {
    "generate a DateTimeFormatter based on yyyyMMdd format" in {
      TimeUtils.DtTimeFormatter.weekIdPattern.toString shouldBe DateTimeFormatter.ofPattern("yyyyMMdd").toString
    }
  }

  "TimeUtils.FromInt.intToLocalDate" should {
    "generate a LocalDate from an Int formatted like yyyyMMdd" in {
      TimeUtils.FromInt.intToLocalDate(20210420) shouldBe LocalDate.of(2021, 4, 20)
    }

    "throw an exception if the input date is not formatted correctly" in {
      the[IllegalArgumentException] thrownBy {
        TimeUtils.FromInt.intToLocalDate(2021420)
      } should have message "requirement failed: date must be an Int respecting the format yyyyMMdd"
    }
  }

  "TimeUtils.FromInt.intToTimestamp" should {
    "generate a Timestamp from an Int formatted like yyyyMMdd" in {
      TimeUtils.FromInt.intToTimestamp(20210420) shouldBe
        Timestamp.valueOf(LocalDate.of(2021, 4, 20).atStartOfDay())
    }

    "throw an exception if the input date is not formatted correctly" in {
      the[IllegalArgumentException] thrownBy {
        TimeUtils.FromInt.intToTimestamp(2021420)
      } should have message "requirement failed: date must be an Int respecting the format yyyyMMdd"
    }
  }

  "TimeUtils.FromTimestamp.tsToLocalDate" should {
    "generate a LocalDate from a timestamp" in {
      val ts = Timestamp.valueOf("2021-04-20 12:00:00")
      TimeUtils.FromTimestamp.tsToLocalDate(ts) shouldBe LocalDate.of(2021, 4, 20)
    }
    "generate a LocalDate from a timestamp at UTC Zone" in {
      val ts = Timestamp.valueOf("2021-04-20 00:00:00")
      // test will pass running mvn test because of the configuration in pom.xml -Duser.timezone=UTC
      TimeUtils.FromTimestamp.tsToLocalDate(ts) shouldBe LocalDate.of(2021, 4, 20)
    }
  }

  "TimeUtils.FromLocaDate.setDateToDay" should {
    "change the day in the week from a certain date" in {
      TimeUtils.FromLocaDate.setDateToDay(TimeUtils.FromLocaDate.mondayDayValue)(LocalDate.of(2021, 4, 20)) shouldBe
        LocalDate.of(2021, 4, 19)
    }
  }

  "TimeUtils.FromLocaDate.setDateToMonday" should {
    "change the date from a certain week to Monday" in {
      TimeUtils.FromLocaDate.setDateToMonday(LocalDate.of(2021, 4, 20)) shouldBe
        LocalDate.of(2021, 4, 19)
    }
  }

  "TimeUtils.FromLocaDate.weekToString" should {
    "turn the LocalDate into a string formatted as yyyyMMdd" in {
      TimeUtils.FromLocaDate.weekToString(LocalDate.of(2021, 4, 20)) shouldBe "20210420"
    }
  }

  "TimeUtils.FromLocaDate.weekToInt" should {
    "turn the LocalDate into an Int formatted as yyyyMMdd" in {
      TimeUtils.FromLocaDate.weekToInt(LocalDate.of(2021, 4, 20)) shouldBe 20210420
    }
  }
}
