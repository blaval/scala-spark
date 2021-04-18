package com.github.blaval.scalaspark.utils.reader

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.github.blaval.scalaspark.utils.TimeUtils.Format

import java.text.SimpleDateFormat

trait YamlMapper {
  protected lazy val getYamlMapper = new ObjectMapper(new YAMLFactory())

  def get: ObjectMapper
}

trait DefaultYamlMapper extends YamlMapper {

  override lazy val get: ObjectMapper = {
    getYamlMapper.registerModule(DefaultScalaModule)
  }
}

trait SpecificRuleMapper extends YamlMapper {

  override lazy val get: ObjectMapper = {
    getYamlMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    getYamlMapper.enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY)
    getYamlMapper.enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY)
    getYamlMapper.registerModule(DefaultScalaModule)
    getYamlMapper.setDateFormat(new SimpleDateFormat(Format.yyyy_MM_dd))
  }
}
