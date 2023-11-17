package com.gdp.domain.model

import java.sql.Timestamp;

case class PolicySatellite(bk: String, cim_src_sys_id: String, cim_oe_id: String, cim_invalid_ts: Timestamp,
                           biz_eff_from_ts: Timestamp, cim_last_modification_ts: Timestamp, cim_load_from_ts: Timestamp,
                           policy_identifier: String, lc_sales_channel_cd: String) extends Satellite {
}
object PolicySatellite {
  val tableName: String = "policy_sat"
}