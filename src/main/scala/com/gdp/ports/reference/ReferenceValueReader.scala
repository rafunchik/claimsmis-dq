package com.gdp.ports.reference

trait ReferenceValueReader {
  def referenceValues(name: String): Set[String]
}
