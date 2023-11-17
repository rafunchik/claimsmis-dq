package com.gdp.ports.storage

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SparkDataFormatSpec extends AnyFlatSpec with Matchers {

  "An SparkDataFormat" should "build from an String" in {
    for (format <- SparkDataFormat.Supported) {
      SparkDataFormat.fromString(format.source) shouldBe Some(format)
    }
  }
}
