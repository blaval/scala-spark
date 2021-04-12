package com.github.blaval.scalaspark.period

import com.github.blaval.scalaspark.model.Period
import com.github.blaval.scalaspark.utils.HiveSpec
import org.scalatest.{Matchers, WordSpec}

import java.time.LocalDate

class PeriodTest extends WordSpec with HiveSpec with Matchers {

  "Period" should {
    "be set up with correct start date before or equal to end date" in {
      the[IllegalArgumentException] thrownBy {
        Period(LocalDate.of(2021, 4, 2), LocalDate.of(2021, 4, 1))
      } should have message "requirement failed: Start must be set before end to define a period"
    }
  }

}
