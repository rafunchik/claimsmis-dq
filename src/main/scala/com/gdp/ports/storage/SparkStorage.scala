package com.gdp.ports.storage

import org.apache.spark.sql.DataFrame

import java.net.URI


trait SparkStorage {

  def loadDataFrame(uri: URI,
                    options: Map[String, String] = Map.empty,
                    format: SparkDataFormat = SparkDataFormat.Parquet): DataFrame

  def saveDataFrame(data: DataFrame,
                    uri: URI,
                    options: Map[String, String] = Map.empty,
                    format: SparkDataFormat = SparkDataFormat.Parquet,
                    saveMode: SaveMode = OverwriteOnlyIfNotComplete): DataFrame

}
