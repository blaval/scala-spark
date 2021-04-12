package com.github.blaval.scalaspark.utils

import org.scalatest.{Matchers, WordSpec}

class EnumComputationTest extends WordSpec with Matchers {
  "EnumComputation.calculateAgeCoef" should {
    "return the right coefficient based on the ageCode" in {
      val enumComp = new EnumComputation()
      enumComp.calculateAgeCoef("very_young") shouldBe 0
      enumComp.calculateAgeCoef("young") shouldBe 1
      enumComp.calculateAgeCoef("adult") shouldBe 2
      enumComp.calculateAgeCoef("elder") shouldBe 3
    }
  }

  "AgeCategory.values" should {
    "contains the list of Enum values ordered" in {
      AgeCategory.values should contain theSameElementsInOrderAs IndexedSeq(
        AgeCategory.VeryYoung,
        AgeCategory.Young,
        AgeCategory.Adult,
        AgeCategory.Elder
      )
    }
  }

}
