package com.github.blaval.scalaspark.utils.reader

import scala.reflect.ClassTag

trait YamlReader { mapper: FileReader with YamlMapper =>

  def readYamlFile[T: ClassTag](path: String): T = {
    val reader    = mapper.read(path)
    val valueType = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]
    mapper.get.readValue(reader, valueType)
  }

}

object DefaultYamlReader extends YamlReader with FileReader with HdfsSource with DefaultYamlMapper

object SpecificRuleYamlReader extends YamlReader with FileReader with HdfsSource with SpecificRuleMapper
