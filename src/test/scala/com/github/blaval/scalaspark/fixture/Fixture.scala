package com.github.blaval.scalaspark.fixture

import com.github.blaval.scalaspark.model.{Patient, Prescription}
import org.scalacheck.Gen

/** Mixin this trait to get sample of various case class */
trait Fixture extends FixtureGenerator {
  implicit lazy val implPatientGen: Gen[Patient]           = patientGenerator
  implicit lazy val implPrescriptionGen: Gen[Prescription] = prescriptionGenerator
}
