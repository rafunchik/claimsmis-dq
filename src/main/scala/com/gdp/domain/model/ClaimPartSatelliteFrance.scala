package com.gdp.domain.model

import java.sql.{Date, Timestamp};

case class ClaimPartSatelliteFrance(bk: String, cim_src_sys_id: String, cim_oe_id: String, cim_invalid_ts: Timestamp,
                              biz_eff_from_ts: Timestamp, cim_last_modification_ts: Timestamp, cim_load_from_ts: Timestamp,
                              claim_part_identifier: String, total_loss_flag: String, lc_settlement_type_cd: String, lc_loss_type_cd: String,
                              lc_business_area_cd: String, lc_line_of_business_cd: String, lc_product_line_cd: String, lc_customer_segment_cd: String,
                              lc_damage_specification_cd: String, lc_claim_assessment_method_cd: String, lc_currency_cd: String,
                              settlement_amount: Double, indemnity_amount: Double, paid_alae_amount: String, recovery_amount: String,
                              salvage_amount: String, claim_open_date: Date, claim_reopen_date: Date, claim_close_date: Date) extends Satellite {

}

object ClaimPartSatelliteFrance {
  val tableName: String = "clm_part_sat"
}
