package com.github.blaval.scalaspark.partitioning

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, RangePartitioner}

case class Patient(name: String, age: Int)

object Example {
  private val NUMBER_OF_PARTITIONS = 3

  def usingDefaultHashPartitioner(patients: RDD[Patient]): RDD[(String, Int)] = {
    patients
      .map(patient => (patient.name, patient.age))
      .partitionBy(new HashPartitioner(NUMBER_OF_PARTITIONS))
  }

  def usingRangePartitioner(patients: RDD[Patient]): RDD[(String, Int)] = {
    val pats = patients.map(patient => (patient.name, patient.age))

    pats
      .partitionBy(new RangePartitioner(NUMBER_OF_PARTITIONS, pats))
  }

  def usingCustomPartitioner(patients: RDD[Patient]): RDD[(String, Int)] = {
    patients
      .map(patient => (patient.name, patient.age))
      .partitionBy(new CustomPartitioner)
  }

}

final class CustomPartitioner extends Partitioner {
  private lazy val numberOfPartitions = 3

  override lazy val numPartitions: Int = numberOfPartitions

  /** returns partition ID from 0 to numPartitions-1 */
  override def getPartition(key: Any): Int = key.toString.length % numPartitions

}
