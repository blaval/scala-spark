package com.github.blaval.scalaspark.dataframe

import com.github.blaval.scalaspark.dataframe.PivotComputation.PatientType._
import com.github.blaval.scalaspark.utils.HiveSpec
import org.scalatest.{Matchers, WordSpec}

class PivotComputationTest extends WordSpec with HiveSpec with Matchers {
  "PivotComputation.calculate" should {
    "compute metrics based on pivot and stack operations" in {
      import spark.implicits._
      val service = new PivotComputation

      val patients =
        Seq(
          (young, 10, 1),
          (adult, 200, 1),
          (elder, 50, 1),
          (young, 10, 1),
          (adult, 200, 1),
          (elder, 50, 2),
          (young, 10, 2),
          (adult, 200, 2),
          (elder, 50, 2)
        ).toDF("patient_type", "count", "provider_id")
      val result = service.calculate(patients)

      val res = result.as[(Int, String, Long)].collect().toList

      val expected =
        Seq((1, young, 20), (1, adult, 400), (1, elder, 50), (2, young, 10), (2, adult, 200), (2, elder, 100))
      res.length shouldBe 6
      res should contain theSameElementsAs expected
    }
  }
}
