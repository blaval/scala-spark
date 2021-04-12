package com.github.blaval.scalaspark.dataset

import com.github.blaval.scalaspark.common.{HasEncoder, Job, TableFunction}
import com.github.blaval.scalaspark.runnable.DatasetArgs
import org.apache.spark.sql.{Dataset, SparkSession}

case class Prescription(id: String, price: Double)

case class Patient(patient_id: Long, prescriptions: Seq[Prescription]) {

  def + (other: Patient): Patient = {
    require(other.patient_id == patient_id, "Cannot merge with another patient because they have different id")
    copy(prescriptions = prescriptions ++ other.prescriptions)
  }
}
object Patient extends HasEncoder[Patient]

class ExampleJob(spark: SparkSession, arguments: DatasetArgs, tableFunction: TableFunction) extends Job {
  implicit val sparkSession: SparkSession = spark
  import spark.implicits.StringToColumn

  override def run(): Unit = {
    import arguments._
    val patients = spark.table(patientTable.name).as[Patient]

    val patientsSelected =
      ExampleJob.selectPatientWithXO1034Prescription(patients)

    tableFunction.overwrite(patientsSelected.repartition(1, $"patient_id"), outputTable)
  }
}

object ExampleJob {
  val prescriptionXO1034 = "XO1034"

  def selectPatientWithXO1034Prescription(patients: Dataset[Patient]): Dataset[Patient] = {
    import patients.sparkSession.implicits.newLongEncoder

    patients
      .groupByKey(_.patient_id)
      .reduceGroups(_ + _)
      .mapPartitions { patients =>
        patients
          .foldLeft(Seq.empty[Patient]) {
            case (acc, (_, patient)) if patient.prescriptions.exists(_.id == prescriptionXO1034) =>
              acc :+ patient
            case (acc, _) => acc
          }
          .toIterator
      }
  }

}
