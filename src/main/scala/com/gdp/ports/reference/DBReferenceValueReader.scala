package com.gdp.ports.reference

import com.typesafe.config.Config
import slick.jdbc.JdbcProfile

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global


class DBReferenceValueReader(referenceValues: Map[String, mutable.Set[String]]) extends ReferenceValueReader {
  def referenceValues(name: String): Set[String] = {
    val codes = referenceValues.get(name)
    if (codes.isDefined) {
      codes.get.toSet
    } else {
      Set()
    }
  }
}

object DBReferenceValueReader {
  def apply(oe: String, dbProfile: JdbcProfile, dbConfig: Config): DBReferenceValueReader = {
    import dbProfile.api._

    var valuesMap: mutable.Map[String, mutable.Set[String]] = new mutable.HashMap[String, mutable.Set[String]]()
    val oeShortName = getOeShortName(oe)
    //use the Slick library and connect to a database using the dbConfig configuration
    val db = dbProfile.api.Database.forConfig("properties", dbConfig)
    val query = sql"""select global_ref_data_name, loc_code from gdp_reference.ref_harmonization
              WHERE global_ref_data_name IN
              ('settlement_type', 'damage_specification', 'product_line', 'line_of_business', 'customer_segment', 'business_area',
              'loss_type', 'claim_assessment_method', 'claim_status', 'claim_process_method', 'sales_channel')
              AND oe_name_short = $oeShortName;""".as[(String, String)]

    db.run(query).map {
      _.foreach {
        case (name, code) => valuesMap.getOrElseUpdate(name, mutable.Set()).add(code)
      }
    }
    new DBReferenceValueReader(valuesMap.toMap)
  }

  private def getOeShortName(oe: String): String = oe match {
    case "GER" =>
      "DE"
    case "FRA" =>
      "FR"
    case "ITA" =>
      "IT"
    case "AUT" =>
      "AT"
    case "ESP" =>
      "ES"
    case _ =>
      null
  }
}


