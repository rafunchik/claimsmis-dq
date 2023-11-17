package com.gdp.domain.dq

import DataQualityChecker.{CriticalError, StructureErrorType}
import com.gdp.ports.storage.DefaultSparkStorage
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec

import java.net.URI
import java.sql.Timestamp


class DataQualityCheckerTest extends AnyFlatSpec with SparkContextSpec {

  import DataQualityCheckerTest._

  "a valid Loss Event satellite" should "pass the checks" in withSparkSession { sparkSession =>

    import sparkSession.implicits._

    val data = Seq(
      ("123", "2012-07-22 22:59:59", "2012-07-22 22:59:59", null, null, 1, 1,
        "loss_event_identifier", "catastrophe_flag", "lc_loss_event_status_cd",
        "2012-07-22 22:59:59", "2012-07-22 22:59:59", "loss_event_name", "lc_loss_event_cause_cd")
    ).toDF(LOSS_EVENT_SATELLITE_COLUMNS:_*)

    val result = DataQualityChecker.runChecks(data, LOSS_EVENT_SATELLITE_TYPE, null)(sparkSession)

    assert(result.count() == 0)
    val validIds = result.select("identifier").collect.map { _.getString(0) }.toSet
    assert(validIds.size == result.count())
  }

  "a claim satellite with an incorrect lc_claim_status_cd" should "not pass the checks" in withSparkSession { sparkSession =>

    val df = new DefaultSparkStorage()(sparkSession).loadDataFrame(
      new URI("src/test/resources/removed.snappy.parquet"),
      Map[String, String]("inferSchema" -> "true"))

    val result = DataQualityChecker.runChecks(df, CLAIM_SATELLITE_TYPE, null)(sparkSession)

    assert(result.count() == 1)
    assert(result.head().get(4) == "The lc_claim_status_cd column should not be null and in OPE,CLO,REO")
  }

  "a Loss Event satellite with a 9999-12-31 biz_date" should "fail the checks" in withSparkSession { sparkSession =>

    import sparkSession.implicits._

    val identifier = "invalid biz date"
    val data = Seq(
      (identifier, "2099-12-31 00:00:00", "9999-12-31 00:00:00", null, null, 1, 1,
        "loss_event_identifier", "catastrophe_flag", "lc_loss_event_status_cd",
        "2012-07-22 22:59:59", "2012-07-22 22:59:59", "loss_event_name", "lc_loss_event_cause_cd")
    ).toDF(LOSS_EVENT_SATELLITE_COLUMNS: _*)

    val result = DataQualityChecker.runChecks(data, LOSS_EVENT_SATELLITE_TYPE, null)(sparkSession)

    assert(result.count() == 1)
    val invalidIds = result.select("identifier").collect.map {
      _.getString(0)
    }.toSet
    assert(invalidIds.size == result.count())
    assert(invalidIds.contains(identifier))
  }

  "invalid Loss Event satellites" should "fail the checks" in withSparkSession { sparkSession =>

    import sparkSession.implicits._

    val data = Seq(
      ("invalid timestamp format", "56.55", "2012-07-22 22:59:59", null, null, 1, 1,
        "loss_event_identifier", "catastrophe_flag", "lc_loss_event_status_cd",
        "2012-07-22 22:59:59", "2012-07-22 22:59:59", "loss_event_name", "lc_loss_event_cause_cd"),
      (null, "2012-07-22 22:59:59", "2012-07-22 22:59:59", null, null, 1, 1,
        "loss_event_identifier", "catastrophe_flag", "lc_loss_event_status_cd",
        "2012-07-22 22:59:59", "2012-07-22 22:59:59", "loss_event_name", "lc_loss_event_cause_cd"),
      ("null cim_load_from_ts", null, "2012-07-22 22:59:59", null, null, 1, 1,
        "loss_event_identifier", "catastrophe_flag", "lc_loss_event_status_cd",
        "2012-07-22 22:59:59", "2012-07-22 22:59:59", "loss_event_name", "lc_loss_event_cause_cd"),
      ("null biz_eff_from_ts", "2012-07-22 22:59:59", null, null, null, 1, 1,
        "loss_event_identifier", "catastrophe_flag", "lc_loss_event_status_cd",
        "2012-07-22 22:59:59", "2012-07-22 22:59:59", "loss_event_name", "lc_loss_event_cause_cd"),
    ).toDF(LOSS_EVENT_SATELLITE_COLUMNS:_*)

    val result = DataQualityChecker.runChecks(data, LOSS_EVENT_SATELLITE_TYPE, null)(sparkSession)

    assert(result.count() == 4)
    val invalidIds = result.select("identifier").collect.map { _.getString(0) }.toSet
    assert(invalidIds.size == result.count())
    assert(invalidIds.contains("invalid timestamp format"))
    assert(invalidIds.contains(null))
    assert(invalidIds.contains("null cim_load_from_ts"))
    assert(invalidIds.contains("null biz_eff_from_ts"))
    val row: Row = result.where($"identifier".isNull).collectAsList().get(0)
    assert(row.getAs[String]("identifier") == null)
    val idColumn = "bk"
    assert(row.getAs[String]("field_name") == idColumn)
    assert(row.getAs[String]("field_value") == null)
    assert(row.getAs[String]("error_type") == StructureErrorType)
    assert(row.getAs[String]("error_text") ==
      s"The $idColumn column should not be null and have a max length of 255 characters")
    assert(row.getAs[String]("error_criticality") == CriticalError)
  }

  "a valid claim part satellite" should "pass the checks" in withSparkSession { sparkSession =>

    val someData = Seq(
      Row("123",
        Timestamp.valueOf("2012-07-22 22:59:59"),
        Timestamp.valueOf("2012-07-22 22:59:59"),
        null,
        null,
        1,
        1,
        "a claim identifier",
        null,
        "CAS",
        "MAD",
        "PC",
        "MOT",
        "MOD",
        "RET",
        "damage",
        null, "EUR", Decimal(0.1), Decimal(0.1), Decimal(0.1), Decimal(0.1), Decimal(0.1), null,
        null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null
      )
    )

    val someSchema = Seq(
      StructField("bk", StringType, false),
      StructField("cim_load_from_ts", TimestampType, false),
      StructField("biz_eff_from_ts", TimestampType, false),
      StructField("cim_last_modification_ts", TimestampType, true),
      StructField("cim_invalid_ts", TimestampType, true),
      StructField("cim_src_sys_id", IntegerType, false),
      StructField("cim_oe_id", IntegerType, false),
      StructField("claim_part_identifier", StringType, false),
      StructField("total_loss_flag", StringType, true),
      StructField("lc_settlement_type_cd", StringType, false),
      StructField("lc_loss_type_cd", StringType, false),
      StructField("lc_business_area_cd", StringType, true),
      StructField("lc_line_of_business_cd", StringType, true),
      StructField("lc_product_line_cd", StringType, true),
      StructField("lc_customer_segment_cd", StringType, false),
      StructField("lc_damage_specification_cd", StringType, true),
      StructField("lc_claim_assessment_method_cd", StringType, true),
      StructField("lc_currency_cd", StringType, false),
      StructField("settlement_amount", DecimalType(18, 2), false),
      StructField("indemnity_amount", DecimalType(18, 2), false),
      StructField("paid_alae_amount", DecimalType(18, 2), false),
      StructField("recovery_amount", DecimalType(18, 2), false),
      StructField("salvage_amount", DecimalType(18, 2), false),
      StructField("paid_repair", DecimalType(18, 2), true),
      StructField("paid_total_loss", DecimalType(18, 2), true),
      StructField("paid_cash_settl", DecimalType(18, 2), true),
      StructField("paid_other", DecimalType(18, 2), true),
      StructField("paid_legal", DecimalType(18, 2), true),
      StructField("reserves", DecimalType(18, 2), true),
      StructField("outstanding_recovery", DecimalType(18, 2), true),
      StructField("paid_labor", DecimalType(18, 2), true),
      StructField("paid_spare_parts", DecimalType(18, 2), true),
      StructField("paid_paint", DecimalType(18, 2), true),
      StructField("paid_subrogation", DecimalType(18, 2), true),
      StructField("paid_medical_expenses", DecimalType(18, 2), true),
      StructField("received_subrogation", DecimalType(18, 2), true),
      StructField("paid_hire", DecimalType(18, 2), true),
      StructField("paid_subrogation_ICA", DecimalType(18, 2), true),
      StructField("received_subrogation_ICA", DecimalType(18, 2), true),
      StructField("claim_incident_flag", StringType, true)
    )

    val data = sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(someData),
      StructType(someSchema)
    )

    val result = DataQualityChecker.runChecks(data, CLAIM_PART_SATELLITE_TYPE, null)(sparkSession)

    assert(result.count() == 0)
    val invalidIds = result.select("identifier").collect.map { _.getString(0) }.toSet
    assert(invalidIds.size == result.count())
  }


  "a valid Hub" should "pass the checks" in withSparkSession { sparkSession =>

    import sparkSession.implicits._

    val data = Seq(
      ("123", "2012-07-22 22:59:59", null, 1, 1)
    ).toDF(HUB_COLUMNS:_*)

    val result = DataQualityChecker.runChecks(data, "claim_hub", null)(sparkSession)
    assert(result.count() == 0)

    val result2 = DataQualityChecker.runChecks(data, "H_CLAIM_PART_AT", null)(sparkSession)
    assert(result2.count() == 0)
  }

  "a telephony satellite with valid data" should "pass the checks" in withSparkSession { sparkSession =>

    import sparkSession.implicits._

    val data = Seq(
      ("123", Timestamp.valueOf("2012-07-22 22:59:59"), Timestamp.valueOf("2012-07-22 22:59:59"), null, null, 1, 1,
        "call_identifier", Timestamp.valueOf("2012-07-22 22:59:59"), "A", "Y", "01:59:59", "call_purpose", null)
    ).toDF(TELEPHONY_SATELLITE_COLUMNS:_*)

    val result = DataQualityChecker.runChecks(data, "telephony_satellite", null)(sparkSession)
    assert(result.count() == 0)
  }

  "a telephony satellite with an invalid call_ring_duration" should "fail the checks" in withSparkSession { sparkSession =>
      import sparkSession.implicits._

      val data = Seq(
      ("123", Timestamp.valueOf("2012-07-22 22:59:59"), Timestamp.valueOf("2012-07-22 22:59:59"), null, null, 1, 1,
        "call_identifier", Timestamp.valueOf("2012-07-22 22:59:59"), "A", "Y", "00:59:5999", "call_purpose", null)
    ).toDF(TELEPHONY_SATELLITE_COLUMNS: _*)

    val result = DataQualityChecker.runChecks(data, "telephony_satellite", null)(sparkSession)
    assert(result.count() == 1)
  }

  "a policy satellite with valid data" should "pass the checks" in withSparkSession { sparkSession =>

    import sparkSession.implicits._

    val data = Seq(
      ("123", Timestamp.valueOf("2012-07-22 22:59:59"), Timestamp.valueOf("2012-07-22 22:59:59"), null, null, 1, 1,
        "policy_identifier", null)
    ).toDF(POLICY_SATELLITE_COLUMNS: _*)

    val result = DataQualityChecker.runChecks(data, POLICY_SATELLITE_TYPE, null)(sparkSession)
    assert(result.count() == 0)
  }

  "a policy satellite with a null policy_identifier" should "fail the checks" in withSparkSession { sparkSession =>

    import sparkSession.implicits._

    val data = Seq(
      ("123", Timestamp.valueOf("2012-07-22 22:59:59"), Timestamp.valueOf("2012-07-22 22:59:59"), null, null, 1, 1,
        null, null)
    ).toDF(POLICY_SATELLITE_COLUMNS: _*)

    val result = DataQualityChecker.runChecks(data, POLICY_SATELLITE_TYPE, null)(sparkSession)
    assert(result.count() == 1)
  }

  "a claim claim_part link with errors" should "fail the checks and report the bk_claim" in withSparkSession { sparkSession =>

    import sparkSession.implicits._

    val bkCLaim = "123"
    val data = Seq(
      (bkCLaim, null, null, null, null, null)
    ).toDF(CLAIM_PART_LINK_COLUMNS: _*)

    val result = DataQualityChecker.runChecks(data, "l_claim_claim_part", null)(sparkSession)
    assert(result.count() == 4)
    val invalidIds = result.select("identifier").collect.map {
      _.getString(0)
    }.toSet
    assert(invalidIds == Set(bkCLaim))
  }

  "a claim loss event link with errors" should "fail the checks and report the bk_claim" in withSparkSession { sparkSession =>

    import sparkSession.implicits._

    val bkCLaim = "123"
    val data = Seq(
      (bkCLaim, null, null, null, null, null)
    ).toDF(LOSS_EVENT_LINK_COLUMNS: _*)

    val result = DataQualityChecker.runChecks(data, "l_claim_loss_event", null)(sparkSession)
    assert(result.count() == 4)
    val invalidIds = result.select("identifier").collect.map {
      _.getString(0)
    }.toSet
    assert(invalidIds == Set(bkCLaim))
  }

  "a claim policy link with errors" should "fail the checks and report the bk_claim" in withSparkSession { sparkSession =>

    import sparkSession.implicits._

    val bkCLaim = "123"
    val data = Seq(
      (bkCLaim, null, null, null, null, null)
    ).toDF(POLICY_LINK_COLUMNS: _*)

    val result = DataQualityChecker.runChecks(data, "l_claim_policy", null)(sparkSession)
    assert(result.count() == 4)
    val invalidIds = result.select("identifier").collect.map {
      _.getString(0)
    }.toSet
    assert(invalidIds == Set(bkCLaim))
  }

  "a french claim satellite" should "have the right schema checks" in {
    val schemaChecks = DataQualityChecker.getChecksByType(null, "S_CIM_CLAIM")
    assert(schemaChecks.size == 18)
  }

  "a french claim hub" should "have the right schema checks" in {
    val schemaChecks = DataQualityChecker.getChecksByType(null, "H_CLAIM")
    assert(schemaChecks.size == 5)
  }

  "a french claim part hub" should "have the right schema checks" in {
    val schemaChecks = DataQualityChecker.getChecksByType(null, "H_CLAIM_PART")
    assert(schemaChecks.size == 5)
  }

  "an italian claim part hub" should "have the right schema checks" in {
    val schemaChecks = DataQualityChecker.getChecksByType(null, "clm_part_hub")
    assert(schemaChecks.size == 5)
  }

  "an austrian claim  hub" should "have the right schema checks" in {
    val schemaChecks = DataQualityChecker.getChecksByType(null, "CLAIMS_HUB_AT")
    assert(schemaChecks.size == 5)
  }

  "an austrian fte hub" should "have the right schema checks" in {
    val schemaChecks = DataQualityChecker.getChecksByType(null, "FTE_HUB_AT")
    assert(schemaChecks.size == 5)
  }

  "an austrian claims hub from 2019" should "have the right schema checks" in {
    val schemaChecks = DataQualityChecker.getChecksByType(null, "CLAIMS_HUB_201912")
    assert(schemaChecks.size == 5)
  }

  "an austrian claims part hub from 2019" should "have the right schema checks" in {
    val schemaChecks = DataQualityChecker.getChecksByType(null, "H_CLAIM_PART_201912")
    assert(schemaChecks.size == 5)
  }

  "an austrian historical claim satellite" should "have the right schema checks" in {
    val schemaChecks = DataQualityChecker.getChecksByType(null, "S_CIM_CLAIM_22")
    assert(schemaChecks.size == 18)
  }

  "a german claim satellite" should "have the right schema checks" in {
    val schemaChecks = DataQualityChecker.getChecksByType(null, "S_CIM_CLAIM")
    assert(schemaChecks.size == 18)
  }

  "a german policy link" should "have the right schema checks" in {
    val schemaChecks = DataQualityChecker.getChecksByType(null, "policy_link")
    assert(schemaChecks.size == 6)
  }

  "a german loss event link" should "have the right schema checks" in {
    val schemaChecks = DataQualityChecker.getChecksByType(null, "loss_event_link")
    assert(schemaChecks.size == 6)
  }

  "an austrian claim satellite" should "have the right schema checks" in {
    val schemaChecks = DataQualityChecker.getChecksByType(null, "S_CIM_CLAIM_AT")
    assert(schemaChecks.size == 18)
  }

  "an austrian historical policy satellite" should "have the right schema checks" in {
    val schemaChecks = DataQualityChecker.getChecksByType(null, "S_CIM_POLICY_22")
    assert(schemaChecks.size == 9)
  }

  "a german policy satellite" should "have the right schema checks" in {
    val schemaChecks = DataQualityChecker.getChecksByType(null, "S_CIM_POLICY")
    assert(schemaChecks.size == 9)
  }

  "an austrian policy satellite" should "have the right schema checks" in {
    val schemaChecks = DataQualityChecker.getChecksByType(null, "S_CIM_POLICY_AT")
    assert(schemaChecks.size == 9)
  }

  "an austrian claim part link" should "have the right schema checks" in {
    val schemaChecks = DataQualityChecker.getChecksByType(null, "L_CLAIM_CLAIM_PART_AT")
    assert(schemaChecks.size == 6)
  }

}

object DataQualityCheckerTest {
  private val LOSS_EVENT_SATELLITE_TYPE = "loss_event_satellite"
  private val CLAIM_PART_SATELLITE_TYPE = "claim_part_satellite"
  private val CLAIM_SATELLITE_TYPE = "claim_satellite"
  private val POLICY_SATELLITE_TYPE = "policy_satellite"
  private val SURVEY_SATELLITE_TYPE = "survey_sat"

  private val LOSS_EVENT_SATELLITE_COLUMNS = Seq("bk", "cim_load_from_ts", "biz_eff_from_ts", "cim_last_modification_ts", "cim_invalid_ts",
    "cim_src_sys_id", "cim_oe_id", "loss_event_identifier", "catastrophe_flag", "lc_loss_event_status_cd",
    "loss_event_date_from", "loss_event_date_to", "loss_event_name", "lc_loss_event_cause_cd")
  private val TELEPHONY_SATELLITE_COLUMNS = Seq("bk", "cim_load_from_ts", "biz_eff_from_ts", "cim_last_modification_ts", "cim_invalid_ts", "cim_oe_id", "cim_src_sys_id",
    "call_identifier", "call_date", "call_incoming_calls", "call_answered_flag", "call_ring_duration",
    "call_purpose", "service_number")
  private val POLICY_SATELLITE_COLUMNS = Seq("bk", "cim_load_from_ts", "biz_eff_from_ts", "cim_last_modification_ts", "cim_invalid_ts", "cim_oe_id", "cim_src_sys_id",
    "policy_identifier", "lc_sales_channel_cd")
  private val HUB_COLUMNS = Seq("bk", "cim_load_from_ts", "cim_invalid_ts", "cim_oe_id", "cim_src_sys_id")
  private val CLAIM_PART_LINK_COLUMNS = Seq("bk_claim", "bk_claim_part", "cim_load_from_ts", "cim_invalid_ts", "cim_oe_id", "cim_src_sys_id")
  private val LOSS_EVENT_LINK_COLUMNS = Seq("bk_claim", "bk_loss_event", "cim_load_from_ts", "cim_invalid_ts", "cim_oe_id", "cim_src_sys_id")
  private val POLICY_LINK_COLUMNS = Seq("bk_claim", "bk_policy", "cim_load_from_ts", "cim_invalid_ts", "cim_oe_id", "cim_src_sys_id")
  private val CLAIM_SATELLITE_COLUMNS = Seq("bk", "cim_load_from_ts", "biz_eff_from_ts", "cim_last_modification_ts", "cim_invalid_ts", "cim_oe_id", "cim_src_sys_id",
    "claim_identifier", "claim_open_date", "claim_reopen_date", "claim_close_date", "date_of_final_settlement",
    "loss_date", "lc_claim_status_cd", "lc_claim_process_method_cd", "lc_currency_cd", "notification_date", "proven_claim_fraud", "fraud_savings_amount")

}