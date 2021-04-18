package com.github.blaval.scalaspark.utils.reader

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import java.io.InputStream

trait FileSource {
  def readFrom(source: String): InputStream
}

trait HdfsSource extends FileSource {
  lazy val getConf = new Configuration()

  def readFrom(source: String): InputStream = {
    val hadoopConf = getConf
    val fs         = FileSystem.get(hadoopConf)
    val filePath   = new Path(source)
    fs.open(filePath)
  }
}
