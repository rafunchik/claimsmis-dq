package com.gdp.ports.storage

import org.apache.spark.sql.functions.{to_date, to_timestamp}
import org.apache.spark.sql.types.{DoubleType, IntegerType}
import org.apache.spark.sql.{DataFrame, SparkSession, SaveMode => SparkSaveMode}

import java.net.URI


object DefaultSparkStorage {

  implicit def sparkStorage(implicit sparkSession: SparkSession): SparkStorage = new DefaultSparkStorage()
}

class DefaultSparkStorage()(implicit sparkSession: SparkSession) extends SparkStorage {

  def loadDataFrame(uri: URI,
                    options: Map[String, String] = Map.empty,
                    format: SparkDataFormat = SparkDataFormat.Parquet): DataFrame = {
    val df = sparkSession.read
      .options(options)
      .format(format.source)
      .load(uri.toString)

    normalizeDataframe(df)
  }

  def saveDataFrame(data: DataFrame,
                    uri: URI,
                    options: Map[String, String] = Map.empty,
                    format: SparkDataFormat = SparkDataFormat.Parquet,
                    saveMode: SaveMode = OverwriteOnlyIfNotComplete): DataFrame = {
    data.write
      .options(options)
      .format(format.source)
      .mode(SparkSaveMode.Overwrite)
      .save(uri.toString)
    data
  }

  private def normalizeDataframe(data: DataFrame) = {
    data
      .columns
      .foldLeft(data) { (data, columnName) =>
        columnName match {
          case columnName if columnName.endsWith("_ts") =>
            data.withColumn(columnName, to_timestamp(data(columnName)))
          case columnName
            if (columnName.endsWith("_date")
              || columnName.startsWith("date_")
              || columnName.equals("loss_event_date_from")
              || columnName.equals("loss_event_date_to")) =>
            data.withColumn(columnName, to_date(data(columnName)))
          case columnName if columnName.endsWith("_amount") =>
            data.withColumn(columnName, data(columnName).cast(DoubleType))
          case columnName if columnName.endsWith("_count") =>
            data.withColumn(columnName, data(columnName).cast(IntegerType))
          case _ => data
        }
      }
      .withColumnRenamed("5_star_rating", "five_star_rating")
  }
}
