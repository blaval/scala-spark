package com.github.blaval.scalaspark.scalaspark.partitioning

import com.github.blaval.scalaspark.scalaspark.utils.HiveSpec
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.{Matchers, WordSpec}

class ExampleTest extends WordSpec with HiveSpec with Matchers {
  "Default partitioner" should {
    "partition based on hash code and number of partitions" in {
      import spark.implicits._
      implicit val sp: SparkSession = spark

      val patients = Seq(Patient("1", 30), Patient("2", 30), Patient("3", 30), Patient("4", 30)).toDS
      val result   = ExampleTest.test(patients, Example.usingDefaultHashPartitioner)
      result should contain theSameElementsAs Seq("(1,30)1", "(2,30)2", "(3,30)0", "(4,30)1")
    }
  }

  "Range partitioner" should {
    "partition based on a range" in {
      import spark.implicits._
      implicit val sp: SparkSession = spark

      val patients = Seq(Patient("1", 30), Patient("2", 30), Patient("3", 30), Patient("4", 30)).toDS
      val result   = ExampleTest.test(patients, Example.usingRangePartitioner)
      result should contain theSameElementsAs Seq("(1,30)0", "(2,30)0", "(3,30)1", "(4,30)2")
    }
  }

  "Custom range partitioner" should {
    "partition based on the length of patient name" in {
      import spark.implicits._
      implicit val sp: SparkSession = spark

      val patients = Seq(Patient("1", 30), Patient("20", 30), Patient("3", 30), Patient("40", 30)).toDS
      val result   = ExampleTest.test(patients, Example.usingCustomPartitioner)
      result should contain theSameElementsAs Seq("(1,30)1", "(20,30)2", "(3,30)1", "(40,30)2")
    }
  }

}

object ExampleTest {

  def test(patients: Dataset[Patient], usePartitioner: RDD[Patient] => RDD[(String, Int)])(implicit
    spark: SparkSession
  ): Seq[String] = {
    val result = usePartitioner(patients.rdd)
      .mapPartitionsWithIndex((partId, pats) => pats.map(_.toString() + partId))

    result.collect()
  }
}
