package com.gdp.domain.dq

import com.amazon.deequ.schema.{RowLevelSchema, RowLevelSchemaValidator}
import com.gdp.domain.model._
import com.gdp.ports.reference.ReferenceValueReader
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

case class DQCheck(errorType: String, fieldName: String, errorText: String, errorCriticality: String, schema: Option[RowLevelSchema], filterPredicate: Option[String]=None)
object DataQualityChecker {

  val StructureErrorType = "Structure & Completeness"
  val CriticalError = "critical"
  val WrongColumnType = "wrong column type"

  val hub = """(?i)(h_[a-z]+)|(?i)([a-z_]+_hub)|(?i)(H_[A-Z_0-9]+)|(?i)([A-Z]+_HUB[A-Z_0-9]*)""".r
  val telephonyHub = """(?i)(H_TELEPHONY[A-Z_0-9]*)|(?i)(TELEPHONY_HUB[A-Z_0-9]*)""".r
  val policyHub = """(?i)(H_POLICY[A-Z_0-9]*)|(?i)(POLICY_HUB[A-Z_0-9]*)""".r
  val claimsHub = """(?i)(H_CLAIM)|(?i)(CLAIMS_HUB[A-Z_0-9]*)|(?i)(CLAIM_HUB[A-Z_0-9]*)""".r
  val claimPartHub = """(?i)(H_CLAIM_PART[A-Z_0-9]*)|(?i)(CLAIM_PART_HUB[A-Z_0-9]*)|(?i)(CLM_PART_HUB[A-Z_0-9]*)""".r
  val lossEventHub = """(?i)(H_LOSS_EVENT[A-Z_0-9]*)|(?i)(LOSS_EVENT_HUB[A-Z_0-9]*)""".r
  val fteHub = """(?i)(H_FTE[A-Z_0-9]*)|(?i)(FTE_HUB[A-Z_0-9]*)""".r
  val lossEventSatellite = """(?i)(loss_event_sat[a-z_0-9]*)|(?i)(S_CIM_LOSS_EVENT[a-z_0-9]*)""".r
  val policySatellite = """(?i)(policy_sat[a-z_0-9]*)|(?i)(S_CIM_POLICY[a-z_0-9]*)""".r
  val telephonySatellite = """(?i)(telephony_sat[a-z_0-9]*)|(?i)(S_CIM_TELEPHONY[a-z_0-9]*)""".r
  val claimSatellite = """(?i)(claim_sat[a-z_0-9]*)|(?i)(S_CIM_CLAIM_[0-9]+)|(?i)(S_CIM_CLAIM[_AT]*)|(?i)(claims_sat[a-z_0-9]*)""".r
  val claimPartSatellite = """(?i)(claim_part_sat[a-z_0-9]*)|(s_cim_claim_part)|(S_CIM_CLAIM_PART_[a-z0-9]*)|(?i)(clm_part_sat[a-z_0-9]*)""".r
  val claimPartSatelliteFrance = """(S_CIM_CLAIM_PART)""".r
  val claimPartLink = """(?i)(l_claim_claim_part[a-z_0-9]*)|(?i)(clm_part_lnk[a-z_0-9]*)|(?i)(clm_part_link[a-z_0-9]*)""".r
  val lossEventLink = """(?i)(l_claim_loss_event[a-z_0-9]*)|(?i)(loss_event_lnk[a-z_0-9]*)|(?i)(loss_event_link[a-z_0-9]*)""".r
  val policyLink = """(?i)(l_claim_policy[a-z_0-9]*)|(?i)(l_policy[a-z_0-9]*)|(?i)(policy_lnk[a-z_0-9]*)|(?i)(policy_link[a-z_0-9]*)""".r
  val telephonyLink = """(?i)(l_claim_telephony[a-z_0-9]*)|(?i)(L_TELEPHONY[a-z_0-9]*)|(?i)(telephony_lnk[a-z_0-9]*)(?i)(telephony_link[a-z_0-9]*)""".r
  val fteSatellite = """(?i)(fte_sat[a-z_0-9]*)|(?i)(FTE_SAT[a-z_0-9]*)""".r
  val surveySatellite = """(?i)(survey_sat)""".r
  val surveyLink = """(?i)(survey_link)""".r
  val surveyHub = """(?i)(survey_hub)""".r


  def asDataset(df: DataFrame, entityType: String)(implicit sparkSession: SparkSession): (Dataset[_ <: CimEntity], String) = {
    import sparkSession.implicits._

    entityType match {
      case lossEventSatellite(_, _) => (df.as[LossEventSatellite], LossEventSatellite.tableName)
      case policySatellite(_, _) => (df.as[PolicySatellite], PolicySatellite.tableName)
      case telephonySatellite(_, _) => (df.as[TelephonySatellite], TelephonySatellite.tableName)
      case claimSatellite(_, _, _, _) => (df.as[ClaimSatellite], ClaimSatellite.tableName)
      case claimPartSatellite(_, _, _, _) => (df.as[ClaimPartSatellite], ClaimPartSatellite.tableName)
      case claimPartSatelliteFrance(_) => (df.as[ClaimPartSatelliteFrance], ClaimPartSatelliteFrance.tableName)
      case claimPartLink(_, _, _) => (df.as[ClaimClaimPartLink], ClaimClaimPartLink.tableName)
      case lossEventLink(_, _, _) => (df.as[ClaimLossEventLink], ClaimLossEventLink.tableName)
      case policyLink(_, _, _, _) => (df.as[ClaimPolicyLink], ClaimPolicyLink.tableName)
      case telephonyLink(_, _, _, _) => (df.as[ClaimTelephonyLink], ClaimTelephonyLink.tableName)
      case fteSatellite(_, _) => (df.as[FTESatellite], FTESatellite.tableName)
      case claimsHub(_, _, _) => (df.as[ClaimHub], ClaimHub.tableName)
      case claimPartHub(_, _, _) => (df.as[ClaimPartHub], ClaimPartHub.tableName)
      case lossEventHub(_, _) => (df.as[LossEventHub], LossEventHub.tableName)
      case policyHub(_, _) => (df.as[PolicyHub], PolicyHub.tableName)
      case telephonyHub(_, _) => (df.as[TelephonyHub], TelephonyHub.tableName)
      case fteHub(_, _) => (df.as[FTEHub], FTEHub.tableName)
      case surveyHub(_) => (df.as[SurveyHub], SurveyHub.tableName)
      case surveySatellite(_) => (df.as[SurveySatellite], SurveySatellite.tableName)
      case surveyLink(_) => (df.as[ClaimSurveyLink], ClaimSurveyLink.tableName)
    }
  }

  def getChecksByType(referenceValuesProvider: ReferenceValueReader, entityType: String): Seq[DQCheck] = {
    val checks = entityType match {
      case lossEventSatellite(_, _) => Seq(
        checkStringColumn(column = "bk", isNullable = false, maxLength = 255),
        checkTimestampColumn("cim_load_from_ts", mask = "yyyy-MM-dd HH:mm:ss", isNullable = false),
        betweenDates("biz_eff_from_ts", isNullable = false),
        checkTimestampColumn("cim_last_modification_ts", mask = "yyyy-MM-dd HH:mm:ss", isNullable = true),
        checkTimestampColumn("cim_invalid_ts", mask = "yyyy-MM-dd HH:mm:ss", isNullable = true),
        checkStringColumn("cim_oe_id", isNullable = false, maxLength = 4),
        checkStringColumn("cim_src_sys_id", isNullable = false, maxLength = 4),
        checkStringColumn(column = "loss_event_identifier", isNullable = false, maxLength = 50),
        checkStringColumn(column = "catastrophe_flag", isNullable = true, maxLength = 50),
        checkStringColumn(column = "lc_loss_event_status_cd", isNullable = false, maxLength = 50),
//        checkIsNotContainedIn(column = "lc_loss_event_status_cd", allowedValues=referenceValuesProvider.referenceValues("loss_event_status"), isNullable = false),
//        checkIsNotContainedIn(column = "lc_loss_event_cause_cd", allowedValues=referenceValuesProvider.referenceValues("loss_event_cause), isNullable = false),
//  removed for now      checkTimestampColumn(column = "loss_event_date_from", mask = "yyyy-MM-dd HH:mm:ss", isNullable = true),
//  removed for now      checkTimestampColumn("loss_event_date_to", mask = "yyyy-MM-dd HH:mm:ss", isNullable = true),
        checkStringColumn(column = "loss_event_name", isNullable = true, maxLength = 50))
      case policySatellite(_, _) => Seq(
        checkStringColumn(column = "bk", isNullable = false, maxLength = 255),
        checkTimestampColumn("cim_load_from_ts", mask = "yyyy-MM-dd HH:mm:ss", isNullable = false),
        betweenDates("biz_eff_from_ts", isNullable = false),
        checkTimestampColumn("cim_last_modification_ts", mask = "yyyy-MM-dd HH:mm:ss", isNullable = true),
        checkTimestampColumn("cim_invalid_ts", mask = "yyyy-MM-dd HH:mm:ss", isNullable = true),
        checkStringColumn("cim_oe_id", isNullable = false, maxLength = 4),
        checkStringColumn("cim_src_sys_id", isNullable = false, maxLength = 4),
        checkStringColumn(column = "policy_identifier", isNullable = false, maxLength = 50),
        checkIsNotContainedIn(column = "lc_sales_channel_cd", allowedValues=referenceValuesProvider.referenceValues("sales_channel"), isNullable = true)
      )
      case telephonySatellite(_, _) => Seq(
        checkStringColumn(column = "bk", isNullable = false, maxLength = 255),
        checkTimestampColumn("cim_load_from_ts", mask = "yyyy-MM-dd HH:mm:ss", isNullable = false),
        betweenDates("biz_eff_from_ts", isNullable = false),
        checkTimestampColumn("cim_last_modification_ts", mask = "yyyy-MM-dd HH:mm:ss", isNullable = true),
        checkTimestampColumn("cim_invalid_ts", mask = "yyyy-MM-dd HH:mm:ss", isNullable = true),
        checkStringColumn("cim_oe_id", isNullable = false, maxLength = 4),
        checkStringColumn("cim_src_sys_id", isNullable = false, maxLength = 4),
        checkStringColumn(column = "call_identifier", isNullable = false, maxLength = 40),
        checkTimestampColumn("call_date", mask = "yyyy-MM-dd HH:mm:ss", isNullable = false),
        checkStringColumn(column = "call_incoming_calls", isNullable = false, maxLength = 1),
        checkStringColumn(column = "call_answered_flag", isNullable = false, maxLength = 1),
        checkStringColumn(column = "call_ring_duration", isNullable = true, maxLength = 8),
        checkStringColumn(column = "call_purpose", isNullable = false, maxLength = 250),
        checkStringColumn(column = "service_number", isNullable = true, maxLength = 16))
      case claimSatellite(_, _, _, _) => Seq(
        checkStringColumn(column = "bk", isNullable = false, maxLength = 255),
        checkTimestampColumn("cim_load_from_ts", mask = "yyyy-MM-dd HH:mm:ss", isNullable = false),
        checkTimestampColumn("biz_eff_from_ts", mask = "yyyy-MM-dd HH:mm:ss", isNullable = false),
        checkTimestampColumn("cim_last_modification_ts", mask = "yyyy-MM-dd HH:mm:ss", isNullable = true),
        checkTimestampColumn("cim_invalid_ts", mask = "yyyy-MM-dd HH:mm:ss", isNullable = true),
        checkStringColumn("cim_oe_id", isNullable = false, maxLength = 4),
        checkStringColumn("cim_src_sys_id", isNullable = false, maxLength = 4),
        checkStringColumn(column = "claim_identifier", isNullable = false, maxLength = 40),
        checkTimestampColumn("claim_open_date", mask = "yyyy-MM-dd HH:mm:ss", isNullable = false),
        checkTimestampColumn("claim_close_date", mask = "yyyy-MM-dd HH:mm:ss", isNullable = true),
        checkTimestampColumn("claim_reopen_date", mask = "yyyy-MM-dd HH:mm:ss", isNullable = true),
        checkTimestampColumn("date_of_final_settlement", mask = "yyyy-MM-dd HH:mm:ss", isNullable = true),
        checkTimestampColumn("loss_date", mask = "yyyy-MM-dd HH:mm:ss", isNullable = true),
        checkIsNotContainedIn(column = "lc_claim_status_cd", allowedValues=referenceValuesProvider.referenceValues("claim_status"), isNullable = false),
//        checkIsNotContainedIn(column = "lc_claim_process_method_cd", allowedValues=Set("OTP", "STP", "ESTP"), isNullable = true),
        checkStringColumn(column = "lc_currency_cd", isNullable = false, maxLength = 3),
        checkTimestampColumn("notification_date", mask = "yyyy-MM-dd HH:mm:ss", isNullable = false),
        checkStringColumn(column = "proven_claim_fraud", isNullable = true, maxLength = 1),
        checkDecimalColumn(column = "fraud_savings_amount", isNullable = true, precision = 18, scale = 2))
      case hub(_, _, _, _) => Seq(
        checkStringColumn(column = "bk", isNullable = false, maxLength = 255),
        checkTimestampColumn("cim_load_from_ts", mask = "yyyy-MM-dd HH:mm:ss", isNullable = false),
        checkTimestampColumn("cim_invalid_ts", mask = "yyyy-MM-dd HH:mm:ss", isNullable = true),
        checkStringColumn("cim_oe_id", isNullable = false, maxLength = 4),
        checkStringColumn("cim_src_sys_id", isNullable = false, maxLength = 4))
      case (claimPartSatellite(_, _, _, _) | claimPartSatelliteFrance(_)) => Seq(
        checkStringColumn(column = "bk", isNullable = false, maxLength = 255),
        checkTimestampColumn("cim_load_from_ts", mask = "yyyy-MM-dd HH:mm:ss", isNullable = false),
        betweenDates("biz_eff_from_ts", isNullable = false),
        checkTimestampColumn("cim_last_modification_ts", mask = "yyyy-MM-dd HH:mm:ss", isNullable = true),
        checkTimestampColumn("cim_invalid_ts", mask = "yyyy-MM-dd HH:mm:ss", isNullable = true),
        checkStringColumn("cim_oe_id", isNullable = false, maxLength = 4),
        checkStringColumn("cim_src_sys_id", isNullable = false, maxLength = 4),
        checkStringColumn(column = "claim_part_identifier", isNullable = false, maxLength = 40),
        checkStringColumn(column = "total_loss_flag", isNullable = true, maxLength = 3),
        checkStringColumn(column = "lc_currency_cd", isNullable = false, maxLength = 3),
        checkDecimalColumn("settlement_amount", precision = 18, scale = 2, isNullable = true),
        checkDecimalColumn("indemnity_amount", precision = 18, scale = 2, isNullable = true),
        checkDecimalColumn("paid_alae_amount", precision = 18, scale = 2, isNullable = true),
        checkDecimalColumn("recovery_amount", precision = 18, scale = 2, isNullable = true),
        checkDecimalColumn("salvage_amount", precision = 18, scale = 2, isNullable = true),
        checkStringColumn(column = "lc_damage_specification_cd", isNullable = false, maxLength = 255),
        checkStringColumn(column = "lc_claim_assessment_method_cd", isNullable = true, maxLength = 255),
      // TODO add inflation columns
        checkIsNotContainedIn(column = "lc_settlement_type_cd", allowedValues=referenceValuesProvider.referenceValues("settlement_type"), isNullable = true),
//        checkIsNotContainedIn(column = "lc_damage_specification_cd", allowedValues=Set[String](), isNullable = false),
        checkIsNotContainedIn(column = "lc_product_line_cd", allowedValues=referenceValuesProvider.referenceValues("product_line"), isNullable = false),
        checkIsNotContainedIn(column = "lc_line_of_business_cd", allowedValues=referenceValuesProvider.referenceValues("line_of_business"), isNullable = false),
        checkIsNotContainedIn(column = "lc_customer_segment_cd", allowedValues=referenceValuesProvider.referenceValues("customer_segment"), isNullable = false),
        checkIsNotContainedIn(column = "lc_business_area_cd", allowedValues=referenceValuesProvider.referenceValues("business_area"), isNullable = false),
        checkIsNotContainedIn(column = "lc_loss_type_cd", allowedValues=referenceValuesProvider.referenceValues("loss_type"), isNullable = false),
//        checkIsNotContainedIn(column = "lc_claim_assessment_method_cd", allowedValues=Set[String]("ONS", "REM", "AUT", "NOA"), isNullable = true),
      )
      case claimPartLink(_, _, _) => Seq(
        checkStringColumn(column = "bk_claim", isNullable = false, maxLength = 255),
        checkStringColumn(column = "bk_claim_part", isNullable = false, maxLength = 255),
        checkTimestampColumn("cim_load_from_ts", mask = "yyyy-MM-dd HH:mm:ss", isNullable = false),
        checkTimestampColumn("cim_invalid_ts", mask = "yyyy-MM-dd HH:mm:ss", isNullable = true),
        checkStringColumn("cim_oe_id", isNullable = false, maxLength = 4),
        checkStringColumn("cim_src_sys_id", isNullable = false, maxLength = 4))
      case lossEventLink(_, _, _) => Seq(
        checkStringColumn(column = "bk_claim", isNullable = false, maxLength = 255),
        checkStringColumn(column = "bk_loss_event", isNullable = false, maxLength = 255),
        checkTimestampColumn("cim_load_from_ts", mask = "yyyy-MM-dd HH:mm:ss", isNullable = false),
        checkTimestampColumn("cim_invalid_ts", mask = "yyyy-MM-dd HH:mm:ss", isNullable = true),
        checkStringColumn("cim_oe_id", isNullable = false, maxLength = 4),
        checkStringColumn("cim_src_sys_id", isNullable = false, maxLength = 4))
      case policyLink(_, _, _, _) => Seq(
        checkStringColumn(column = "bk_claim", isNullable = false, maxLength = 255),
        checkStringColumn(column = "bk_policy", isNullable = false, maxLength = 255),
        checkTimestampColumn("cim_load_from_ts", mask = "yyyy-MM-dd HH:mm:ss", isNullable = false),
        checkTimestampColumn("cim_invalid_ts", mask = "yyyy-MM-dd HH:mm:ss", isNullable = true),
        checkStringColumn("cim_oe_id", isNullable = false, maxLength = 4),
        checkStringColumn("cim_src_sys_id", isNullable = false, maxLength = 4))
      case telephonyLink(_, _, _, _) => Seq(
        checkStringColumn(column = "bk_claim", isNullable = false, maxLength = 255),
        checkStringColumn(column = "bk_telephony", isNullable = false, maxLength = 255),
        checkTimestampColumn("cim_load_from_ts", mask = "yyyy-MM-dd HH:mm:ss", isNullable = false),
        checkTimestampColumn("cim_invalid_ts", mask = "yyyy-MM-dd HH:mm:ss", isNullable = true),
        checkStringColumn("cim_oe_id", isNullable = false, maxLength = 4),
        checkStringColumn("cim_src_sys_id", isNullable = false, maxLength = 4))
      case fteSatellite(_, _) => Seq(
        checkStringColumn(column = "bk", isNullable = false, maxLength = 255),
        checkTimestampColumn("cim_load_from_ts", mask = "yyyy-MM-dd HH:mm:ss", isNullable = false),
        betweenDates("biz_eff_from_ts", isNullable = false),
        checkTimestampColumn("cim_last_modification_ts", mask = "yyyy-MM-dd HH:mm:ss", isNullable = true),
        checkTimestampColumn("cim_invalid_ts", mask = "yyyy-MM-dd HH:mm:ss", isNullable = true),
        checkStringColumn("cim_oe_id", isNullable = false, maxLength = 4),
        checkStringColumn("cim_src_sys_id", isNullable = false, maxLength = 4)
      )
      case surveyLink(_) => Seq(
        checkStringColumn(column = "bk_claim", isNullable = false, maxLength = 255),
        checkStringColumn(column = "bk_survey", isNullable = false, maxLength = 255),
        checkTimestampColumn("cim_load_from_ts", mask = "yyyy-MM-dd HH:mm:ss", isNullable = false),
        checkTimestampColumn("cim_invalid_ts", mask = "yyyy-MM-dd HH:mm:ss", isNullable = true),
        checkStringColumn("cim_oe_id", isNullable = false, maxLength = 4),
        checkStringColumn("cim_src_sys_id", isNullable = false, maxLength = 4))
      case surveySatellite(_) => Seq(
        checkStringColumn(column = "bk", isNullable = false, maxLength = 255),
        checkTimestampColumn("cim_load_from_ts", mask = "yyyy-MM-dd HH:mm:ss", isNullable = false),
        betweenDates("biz_eff_from_ts", isNullable = false),
        checkTimestampColumn("cim_last_modification_ts", mask = "yyyy-MM-dd HH:mm:ss", isNullable = true),
        checkTimestampColumn("cim_invalid_ts", mask = "yyyy-MM-dd HH:mm:ss", isNullable = true),
        checkStringColumn("cim_oe_id", isNullable = false, maxLength = 4),
        checkStringColumn("cim_src_sys_id", isNullable = false, maxLength = 4),
        checkStringColumn(column = "survey_identifier", isNullable = false, maxLength = 100),
        checkStringColumn(column = "survey_purpose", isNullable = true, 50),
        checkTimestampColumn(column = "date_of_survey", mask = "yyyy-MM-dd HH:mm:ss", isNullable = false),
        checkTimestampColumn(column = "date_of_submission", mask = "yyyy-MM-dd HH:mm:ss", isNullable = false),
//        checkStringColumn(column = "five_star_rating", isNullable = false, 10)
      )
    }

    checks
  }

  private def checkStringColumn(column: String, isNullable: Boolean, maxLength: Integer) = {
    val errorText = if (isNullable) {
      s"The $column column should have a max length of $maxLength characters"
    }
    else {
      s"The $column column should not be null and have a max length of $maxLength characters"
    }
    DQCheck(StructureErrorType, column, errorText, CriticalError, Some(RowLevelSchema().withStringColumn(
      name = column, isNullable = isNullable, maxLength = Some(maxLength))))
  }

  private def checkTimestampColumn(column: String, mask: String, isNullable: Boolean) = {
    val errorText = if (isNullable) {
      s"The $column column should be a timestamp with format $mask"
    }
    else {
      s"The $column column should be a non null timestamp with format $mask"
    }
    DQCheck(StructureErrorType, column, errorText, CriticalError,
      Some(RowLevelSchema().withTimestampColumn(name = column, mask = mask, isNullable = isNullable)))
  }

  private def checkDecimalColumn(column: String, precision: Int, scale: Int, isNullable: Boolean) = {
    val errorText = if (isNullable) {
      s"The $column column should have a $precision precision and $scale scale"
    }
    else {
      s"The $column column should not be null and have a $precision precision and $scale scale"
    }
    DQCheck(StructureErrorType, column, errorText, CriticalError, Some(RowLevelSchema().withDecimalColumn(
      name = column, precision, scale, isNullable)))
  }

  private def checkIsNotContainedIn(column: String, allowedValues: Set[String], isNullable: Boolean) = {
    val valueList = allowedValues
      .map {
        _.replaceAll("'", "''")
      }
      .mkString("'", "','", "'")
    val tuple = if (isNullable) {
      (s"The $column column should be in ${allowedValues.mkString(",")}",
        s"`$column` NOT IN ($valueList)")
    }
    else {
      (s"The $column column should not be null and in ${allowedValues.mkString(",")}",
        s"`$column` IS NULL OR `$column` NOT IN ($valueList)")
    }
    val errorText = tuple._1
    val predicate = tuple._2
    DQCheck("Business checks", column, errorText, CriticalError, None, Some(predicate))
  }

  private def betweenDates(column: String,
                           after: String = "1900-12-31 00:00:00",
                           before: String = "9999-12-30 00:00:00",
                           isNullable: Boolean) = {
    //write a spark filter predicate to check if the column is between the two dates
    val tuple = if (isNullable) {
      (s"The $column column should be between $after and $before",
        s"`$column` < '$after' OR `$column` > '$before'")
    }
    else {
      (s"The $column column should not be null and between $after and $before",
        s"`$column` IS NULL OR `$column` < '$after' OR `$column` > '$before'")
    }
    val errorText = tuple._1
    val predicate = tuple._2
    DQCheck(StructureErrorType, column, errorText, CriticalError, None, Some(predicate))
  }

  def runChecks(data: DataFrame, entityType: String, referenceValueReader: ReferenceValueReader)(implicit sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    val rowIdentifier = "identifier"
    val schemaChecks: Seq[DQCheck] = getChecksByType(referenceValueReader, entityType)
    val result = schemaChecks.map { check =>
      val res: DataFrame = try {
        check.schema match {
          case Some(schema) => RowLevelSchemaValidator
            .validate (data, schema)
            .invalidRows
            .withColumn("field_value", col(check.fieldName))
            .withColumnRenamed("bk_claim", rowIdentifier)
            .withColumnRenamed("bk", rowIdentifier)
            .select(
              col(rowIdentifier),
              lit(check.fieldName).as("field_name"),
              col("field_value"),
              lit(check.errorType).as("error_type"),
              lit(check.errorText).as("error_text"),
              lit(check.errorCriticality).as("error_criticality"))
          case None => check.filterPredicate match {
            case Some(predicate) =>
              data
                .filter(predicate)
                .withColumn("field_value", col(check.fieldName))
                .withColumnRenamed("bk_claim", rowIdentifier)
                .withColumnRenamed("bk", rowIdentifier)
                .select(
                  col(rowIdentifier),
                  lit(check.fieldName).as("field_name"),
                  col("field_value"),
                  lit(check.errorType).as("error_type"),
                  lit(check.errorText).as("error_text"),
                  lit(check.errorCriticality).as("error_criticality"))
            case None => ??? //TODO add logging
          }
        }
      }
      catch {
        case e: Exception => {
          Seq(
            (WrongColumnType, check.fieldName, "wrong column type", check.errorType, check.errorText, check.errorCriticality)
          ).toDF(rowIdentifier, "field_name", "field_value", "error_type", "error_text", "error_criticality")
        }
      }
      res
    }.reduce((dfx, dfy) => dfx.union(dfy))
    result
  }

}
