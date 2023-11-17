package com.gdp.domain.model

import java.sql.{Date, Timestamp};

case class ClaimSatellite(bk: String, cim_src_sys_id: String, cim_oe_id: String, cim_invalid_ts: Timestamp,
                          biz_eff_from_ts: Timestamp, cim_last_modification_ts: Timestamp, cim_load_from_ts: Timestamp,
                          claim_identifier: String, claim_open_date: Date, claim_reopen_date: Date, claim_close_date: Date, date_of_final_settlement: Date,
                          loss_date: Date, lc_claim_status_cd: String, lc_claim_process_method_cd: String, lc_currency_cd: String, notification_date: Date,
                          proven_claim_fraud: String, fraud_savings_amount: Double) extends Satellite {
}

object ClaimSatellite {
  val tableName: String = "claims_sat"
}