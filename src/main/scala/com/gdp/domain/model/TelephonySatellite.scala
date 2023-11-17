package com.gdp.domain.model

import java.sql.{Date, Timestamp};

case class TelephonySatellite(bk: String, cim_src_sys_id: String, cim_oe_id: String, cim_invalid_ts: Timestamp,
                              biz_eff_from_ts: Timestamp, cim_last_modification_ts: Timestamp, cim_load_from_ts: Timestamp,
                              call_identifier: String, call_date: Date, call_incoming_calls: String, call_answered_flag: String, call_ring_duration: Timestamp,
                              call_purpose: String, service_number: String) extends Satellite {
}

object TelephonySatellite {
  val tableName: String = "telephony_sat"
}
