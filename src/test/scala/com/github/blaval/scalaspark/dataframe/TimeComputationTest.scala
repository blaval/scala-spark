package com.github.blaval.scalaspark.dataframe

import com.github.blaval.scalaspark.model.Period
import com.github.blaval.scalaspark.utils.HiveSpec
import com.github.blaval.scalaspark.utils.TimeUtils.FromInt.{intToTimestamp => toTs}
import org.scalatest.{Matchers, WordSpec}

import java.sql.Timestamp
import java.time.LocalDate

case class PatientSelectedTest(service_date: Timestamp, service_date_end: Timestamp, service_duration: Int)

class TimeComputationTest extends WordSpec with HiveSpec with Matchers {
  "TimeComputation.calculate" should {
    "select patients based on date restrictions" in {
      import spark.implicits._

      val start       = LocalDate.of(2021, 4, 4)
      val end         = LocalDate.of(2021, 4, 10)
      val period      = Period(start, end)
      val timeService = new TimeComputation(spark, period)

      val patients = Seq(
        // will be ignored because out of selection period
        (toTs(20210403), toTs(20210409)),
        (toTs(20210404), toTs(20210409)),
        (toTs(20210410), toTs(20210412)),
        // will be ignored because out of selection period
        (toTs(20210411), toTs(20210412))
      ).toDF("service_date", "service_date_end")
      val result = timeService.calculate(patients)

      val expected =
        Seq((toTs(20210404), toTs(20210409), 5), (toTs(20210410), toTs(20210412), 2))
      val res = result.as[(Timestamp, Timestamp, Int)].collect()
      res.length shouldBe 2
      res should contain theSameElementsAs expected
    }
  }
}
