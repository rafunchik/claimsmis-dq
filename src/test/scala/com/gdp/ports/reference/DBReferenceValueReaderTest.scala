package com.gdp.ports.reference

import com.typesafe.config.ConfigFactory
import org.scalatest.flatspec.AnyFlatSpec

class DBReferenceValueReaderTest extends AnyFlatSpec {

 "apply" should "initialize a DBReferenceValueReader correctly" in {
   val dbConfig = ConfigFactory.load("test-application.conf").getConfig("db")
   assert(dbConfig.getString("properties.secret.scope").equals("test-scope"))

   val reader = DBReferenceValueReader("it", slick.jdbc.H2Profile, dbConfig)
   assert(reader.referenceValues("name").isEmpty)

  }

}
