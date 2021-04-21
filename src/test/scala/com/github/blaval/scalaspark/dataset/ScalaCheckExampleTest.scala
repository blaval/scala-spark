package com.github.blaval.scalaspark.dataset

import com.github.blaval.scalaspark.fixture.Fixture
import com.github.blaval.scalaspark.model.{Patient, Prescription}
import com.github.blaval.scalaspark.utils.HiveSpec
import org.scalatest.{Matchers, WordSpec}

class ScalaCheckExampleTest extends WordSpec with HiveSpec with Matchers with Fixture {

  "ScalaCheckExample" should {
    "give an example how to use Scala Check Generator to generate random fixtures" in {
      val patient: Patient           = getSample
      val prescription: Prescription = getSample

      patient.patient_id should be >= 0L
      prescription.price should be >= 0.0
    }
    "give an example how to use Scala Check Generator to generate random N fixtures" in {
      val numPatients            = 5
      val patients: Seq[Patient] = getNSamples(numPatients)

      patients.size shouldBe numPatients
      patients.map(_.patient_id should be >= 0L)
    }
  }
}
