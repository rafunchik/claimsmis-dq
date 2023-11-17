package com.gdp.ports.storage

sealed trait SparkDataFormat {
  def source: String
}

object SparkDataFormat {

  case object Parquet extends SparkDataFormat {
    val source = "parquet"
  }

  case object Json extends SparkDataFormat {
    val source = "json"
  }

  case object Csv extends SparkDataFormat {
    val source = "com.databricks.spark.csv"
  }

  case object Redshift extends SparkDataFormat {
    val source = "com.databricks.spark.redshift"
  }

  val Supported = Set(Parquet, Json, Redshift, Csv)

  val DefaultDataFormat: SparkDataFormat = Parquet

  def fromString(format: String): Option[SparkDataFormat] = format.toLowerCase match {
    case Parquet.source => Some(Parquet)
    case Json.source => Some(Json)
    case Redshift.source => Some(Redshift)
    case Csv.source => Some(Csv)
    case _ => None
  }
}
