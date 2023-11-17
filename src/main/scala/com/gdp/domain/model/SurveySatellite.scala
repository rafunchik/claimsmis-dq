package com.gdp.domain.model

import java.sql.{Date, Timestamp};

case class SurveySatellite(bk: String, cim_src_sys_id: String, cim_oe_id: String, cim_invalid_ts: Timestamp,
                           biz_eff_from_ts: Timestamp, cim_last_modification_ts: Timestamp, cim_load_from_ts: Timestamp,
                           survey_identifier: String, survey_purpose: String, date_of_survey: Date, date_of_submission: Date,
                           five_star_rating: String) extends Satellite
object SurveySatellite {
  val tableName: String = "survey_sat"
}
