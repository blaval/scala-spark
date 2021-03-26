package com.github.blaval.scalaspark.scalaspark.common

import org.apache.spark.sql.{Encoder, Encoders}

import scala.reflect.runtime.universe.TypeTag

trait HasEncoder[T <: Product] {
  implicit def encoder(implicit typeTag: TypeTag[T]): Encoder[T] = Encoders.product[T]
}
