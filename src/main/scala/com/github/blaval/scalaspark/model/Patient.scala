package com.github.blaval.scalaspark.model

import com.github.blaval.scalaspark.common.HasEncoder

case class Patient(patient_id: Long, prescriptions: Seq[Prescription]) {

  def + (other: Patient): Patient = {
    require(other.patient_id == patient_id, "Cannot merge with another patient because they have different id")
    copy(prescriptions = prescriptions ++ other.prescriptions)
  }
}

object Patient extends HasEncoder[Patient]
