package com.github.blaval.scalaspark.fixture

import com.github.blaval.scalaspark.model.{Patient, Prescription}
import org.scalacheck.Gen

trait PrescriptionFixtureGen {

  lazy val prescriptionGenerator: Gen[Prescription] = for {
    id    <- Gen.alphaNumStr
    price <- Gen.posNum[Double]
  } yield Prescription(id, price)
}

trait PatientFixtureGen extends PrescriptionFixtureGen {

  lazy val patientGenerator: Gen[Patient] = for {
    patientId     <- Gen.posNum[Long]
    prescriptions <- Gen.listOf(prescriptionGenerator)
  } yield Patient(patient_id = patientId, prescriptions)
}

trait FixtureGenerator extends GeneratorUtils with PatientFixtureGen with PrescriptionFixtureGen
