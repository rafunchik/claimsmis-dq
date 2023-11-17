package com.gdp.domain.model

import java.sql.Timestamp;

case class FTESatellite(bk: String, cim_src_sys_id: String, cim_oe_id: String, cim_invalid_ts: Timestamp,
                        biz_eff_from_ts: Timestamp, cim_last_modification_ts: Timestamp, cim_load_from_ts: Timestamp,
                        claims_internal_fte_count: Integer, claims_external_fte_count: Integer, claims_voluntarily_leavers_count: Integer) extends Satellite {
}
object FTESatellite {
  val tableName: String = "fte_sat"
}