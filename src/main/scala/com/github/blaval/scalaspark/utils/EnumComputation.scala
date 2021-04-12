package com.github.blaval.scalaspark.utils

import enumeratum.EnumEntry
import enumeratum.EnumEntry.Snakecase

import scala.collection.immutable

final class EnumComputation {

  def calculateAgeCoef(ageCode: String): Int = {
    AgeCategory.withNameInsensitive(ageCode) match {
      case AgeCategory.VeryYoung => 0
      case AgeCategory.Young     => 1
      case AgeCategory.Adult     => 2
      case AgeCategory.Elder     => 3
    }
  }
}

import enumeratum.Enum
sealed trait AgeCategory extends EnumEntry with Snakecase

object AgeCategory extends Enum[AgeCategory] {
  override val values: immutable.IndexedSeq[AgeCategory] = findValues

  case object VeryYoung extends AgeCategory
  case object Young     extends AgeCategory
  case object Adult     extends AgeCategory
  case object Elder     extends AgeCategory
}
