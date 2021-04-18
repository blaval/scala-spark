package com.github.blaval.scalaspark.utils.reader

import java.io.{BufferedReader, InputStream, InputStreamReader}

trait FileReader {
  source: FileSource =>

  def readStream[T <: InputStream](inputStream: T): BufferedReader =
    new BufferedReader(new InputStreamReader(inputStream))

  def read[T](path: String): BufferedReader =
    readStream(source.readFrom(path))
}
