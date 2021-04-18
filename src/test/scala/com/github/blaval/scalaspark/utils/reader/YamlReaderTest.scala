package com.github.blaval.scalaspark.utils.reader

import org.scalatest.{Matchers, WordSpec}

import java.time.{LocalDate, LocalDateTime, LocalTime, ZoneOffset}
import java.util.Date

case class PrescriptionsCondition(code: String, in: Option[Seq[String]], like: Option[Seq[String]])
case class Patients(prescriptions: Seq[PrescriptionsCondition], address: String, event: Option[Date])
case class Physicians(ageOver: Int)
case class YamlConfigTest(patients: Patients, physicians: Physicians)

class YamlReaderTest extends WordSpec with Matchers {
  "DefaultYamlReader.readYamlFile" should {
    "read and parse correctly a yaml file from DefaultYamlReader" in {
      val inputFilePath          = "src/test/resources/yaml_config.yaml"
      val result: YamlConfigTest = DefaultYamlReader.readYamlFile[YamlConfigTest](inputFilePath)
      result shouldBe YamlConfigTest(
        Patients(
          Seq(
            PrescriptionsCondition("code1", in = Some(Seq("A", "C")), None),
            PrescriptionsCondition("code2", None, like = Some(Seq("D%", "B%")))
          ),
          address = "221B Baker Street",
          event = Some(Date.from(LocalDateTime.of(2021, 4, 20, 0, 0, 0).toInstant(ZoneOffset.UTC)))
        ),
        Physicians(50)
      )
    }
  }
  "SpecificRuleYamlReader.readYamlFile" should {
    "read and parse correctly a yaml file from SpecificRuleYamlReader" in {
      val inputFilePath          = "src/test/resources/yaml_specific_rule_config.yaml"
      val result: YamlConfigTest = SpecificRuleYamlReader.readYamlFile[YamlConfigTest](inputFilePath)
      result shouldBe YamlConfigTest(
        Patients(
          Seq(
            PrescriptionsCondition("code1", in = Some(Seq("A", "C")), None),
            PrescriptionsCondition("code2", None, like = Some(Seq("D%", "B%")))
          ),
          address = "221B Baker Street",
          event = Some(Date.from(LocalDateTime.of(LocalDate.of(2021, 4, 20), LocalTime.MIN).toInstant(ZoneOffset.UTC)))
        ),
        Physicians(50)
      )
    }
  }

}
