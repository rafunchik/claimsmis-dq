package com.gdp.domain.model

import java.sql.Timestamp;

case class ClaimSurveyLink(cim_src_sys_id: String, cim_oe_id: String, cim_invalid_ts: Timestamp, bk_claim: String, bk_survey: String) extends CimEntity {
}
object ClaimSurveyLink {
  val tableName: String = "survey_lnk"
}
