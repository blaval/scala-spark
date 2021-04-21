package com.github.blaval.scalaspark.fixture

import org.scalacheck.{Arbitrary, Gen}

trait GeneratorUtils {
  def getSample[A: Gen]: A                = implicitly[Gen[A]].sample.get
  def getNSamples[A: Gen](n: Int): Seq[A] = (1 to n) map (_ => implicitly[Gen[A]].sample.get)
}

object GeneratorUtils {

  def optionGen[T](otherGen: Gen[T]): Gen[Option[T]] =
    Arbitrary.arbOption(Arbitrary(otherGen)).arbitrary
}
