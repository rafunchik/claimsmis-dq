package com.gdp

import com.gdp.domain.dq.DataQualityChecker
import com.gdp.ports.DatabricksSecretManager
import com.gdp.ports.reference.DBReferenceValueReader
import com.gdp.ports.report.DBCheckResultWriter
import com.gdp.ports.storage.{DefaultSparkStorage, SparkStorage}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
import slick.jdbc.PostgresProfile

import java.net.URI
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import scala.collection.mutable
import scala.language.existentials
import scala.util.matching.Regex

object DQJob {
  protected val appName: String = this.getClass.getName.split('$').last
  private val logger: Logger = LoggerFactory.getLogger(appName)
  private val secretManager = DatabricksSecretManager

  private val spId = secretManager.getSecret(scope = "...scope", key = "...id")
  private val spSecret = secretManager.getSecret(scope = "...scope", key = "...key")

  //TODO move to conf
  private val DefaultSparkSettings: Map[String, String] = Map[String, String](
    "fs.azure.account.auth.type..." -> "OAuth",
    "fs.azure.account.oauth.provider.type..." -> "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id...." -> spId,
    "fs.azure.account.oauth2.client.secret...." -> spSecret,
    "fs.azure.account.oauth2.client.endpoint...." -> "https://login.microsoftonline.com/..."
  )

  private def writeSuccessfulFiles(storage: SparkStorage, goodData: mutable.Map[String, DataFrame], oe: String, azureConfig: Config)(implicit sparkSession: SparkSession): Unit = {
    //TODO move to diff class
    val ts = new SimpleDateFormat("yyyyMMddHHmmss").format(new java.util.Date());
    val path = azureConfig.getString("datalakePath")
    goodData.foreach { case (entityType, df) =>
      if (df.count() > 0) {
        val (dataset, entity) = DataQualityChecker.asDataset(df, entityType)
        storage.saveDataFrame(dataset.toDF(), new URI(s"${path}dq_passed/$oe/$entity/${ts}-${entityType}/"))
        storage.saveDataFrame(dataset.toDF(), new URI(s"${path}clean/$oe/$entity/${ts}-${entityType}/")) //TODO partition by year, month, etc
      }
      else {
        logger.warn(s"Empty parquet file: $oe-$entityType")
      }
    }
  }

  def reportingDate(): Date = {
    val monthAsInt = Calendar.getInstance.get(Calendar.MONTH)
    val reportingMonth = if (monthAsInt > 0) monthAsInt - 1 else 11
    val yearAsInt = Calendar.getInstance.get(Calendar.YEAR)
    val reportingYear = if (monthAsInt > 0) yearAsInt else yearAsInt - 1
    new Date(Timestamp.valueOf(s"${reportingYear}-${reportingMonth}-1 00:00:00").getTime)
  }

  def main(args: Array[String]): Unit = {
    implicit val sparkSession: SparkSession = createSession(DefaultSparkSettings, None)
    val path = args(0)
    val tables: Array[String] = args(1).split(",")
    val config = ConfigFactory.load
    val storage = new DefaultSparkStorage()
    val dbConfig = config.getConfig("db")
    val resultWriter = new DBCheckResultWriter(secretManager, dbConfig.getConfig("properties"))
    val oe = DQJobUtils.extractOEFromPath(path)
    val referenceValueReader = DBReferenceValueReader(oe, PostgresProfile, dbConfig)
    val effectiveDate: Date = reportingDate()
    val goodData: mutable.Map[String, DataFrame] = mutable.Map()
    var fail = false
    tables.foreach { tableName =>
      val filePath = DQJobUtils.expandPath(oe, tableName, config.getConfig("azure"))
      val df = storage.loadDataFrame(
        uri = new URI(filePath),
        options = Map[String, String]("inferSchema" -> "true"))

      val result = DataQualityChecker.runChecks(df, tableName, referenceValueReader)
      if (result.count() > 0) {
        fail = true
        val numRows = df.count()
        resultWriter.writeDQFailure(oe, tableName, effectiveDate, result, numRows)
      }
      else {
        goodData.put(tableName, df)
      }
      println(s"OE: ${oe}, ${tableName}:")
      println(s"${result.select("field_name", "field_value", "error_text").distinct().show(truncate=false)}")
    }
    if (fail) {
      throw new RuntimeException("Data quality checks failed")
    }
    else {
      writeSuccessfulFiles(storage, goodData, oe, config.getConfig("azure"))
    }
  }

  private def createSession(sparkSettings: Map[String, String], parallelism: Option[Int]): SparkSession = {
    val defaultSessionBuilder = SparkSession
      .builder()
      .enableHiveSupport()
      .appName(appName)

    val settingsWithParallelism = parallelism.fold(sparkSettings) { parallelism =>
      sparkSettings + ("spark.default.parallelism" -> parallelism.toString)
    }

    //    logger.info(s"Creating Spark session with config: {}",
    //      settingsWithParallelism.map({ case (k, v) => s"$k=$v" }).mkString(", "))

    settingsWithParallelism.foldLeft(defaultSessionBuilder) {
      case (builder, (key, value)) => builder.config(key, value)
    }.getOrCreate()
  }
}

object DQJobUtils {

  def expandPath(oe: String, table: String, azureConfig: Config): String = {
    s"${azureConfig.getString("datalakePath")}raw_parquet/$oe/$table/$table.snappy.parquet"
  }

  def extractOEFromPath(path: String): String = {
    val keyValPattern: Regex = "raw_parquet/([a-z][a-z])/".r
    val patternMatch = keyValPattern.findFirstMatchIn(path).get
    val oe = patternMatch.group(1)
    oe
  }
}