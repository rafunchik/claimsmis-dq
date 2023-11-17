package com.gdp.ports.report

import com.gdp.ports.SecretManager
import com.typesafe.config.Config
import org.apache.commons.codec.digest.DigestUtils
import org.apache.spark.SparkException
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import java.util.Date;

class DBCheckResultWriter(secretManager: SecretManager, config: Config) extends CheckResultWriter {

    private val logger = LoggerFactory.getLogger("DBCheckResultWriter")

    private def getCimOeId(oe: String): Int = oe match {
        case "de" =>
            2
        case "fr" =>
            1
        case "it" =>
            6
        case "at" =>
            4
        case "es" =>
            5
        case _ =>
            -1
    }
    def writeDQFailure(oe: String, source: String, bizzEffectiveDate: Date, results: DataFrame, numRows: Long)(implicit sparkSession: SparkSession): Unit = {

        import sparkSession.implicits._

        val hk: String = submissionInfoHk(oe, source, bizzEffectiveDate)
        val failures: DataFrame = results.withColumn("hk_submission", lit(hk))
        val loadTimestamp = new java.sql.Timestamp(System.currentTimeMillis())
        val functionalAreaId = 1 //TODO hardcoded to "CLAIMS", use mapping in DB to change to Telephony, etc.

        val submissionInfo = Seq(
            (hk, oe, getCimOeId(oe), functionalAreaId, source, loadTimestamp, numRows, new java.sql.Date(bizzEffectiveDate.getTime))
        ).toDF("hk", "oe_short_name",  "organization_id", "functional_area_id", "source",
            "load_timestamp", "num_rows", "reporting_period")
        val params = connectionParams(config)
        try {
            submissionInfo
              .write
              .format("jdbc")
              .options(params + ("dbtable" -> "gdp_dq.dq_submission_info"))
              .mode("append")
              .save()
        } catch {
            case e: SparkException =>
                logger.debug(s"Already wrote submission info for $oe, $source, $bizzEffectiveDate", e)
        }
        failures
          .write
          .format("jdbc")
          .options(params + ("dbtable" -> "gdp_dq.dq_error"))
          .mode("append")
          .save()
    }

    private def connectionParams(config: Config): Map[String, String] = {
        Map(
            "driver" -> config.getString("driver"),
            "url" -> config.getString("url"),
            "user" -> config.getString("user"),
            "sslmode" -> config.getString("sslmode"),
            "reWriteBatchedInserts" -> config.getString("reWriteBatchedInserts"),
            "password" -> secretManager.getSecret(
                scope = config.getString("secret.scope"),
                key = config.getString("secret.key"))
        )
    }

    private def submissionInfoHk(oe: String, source: String, bizzEffectiveDate: Date): String = {
        DigestUtils.md5Hex(s"${oe}~${source}~${bizzEffectiveDate.toString}").toLowerCase()
    }
}
