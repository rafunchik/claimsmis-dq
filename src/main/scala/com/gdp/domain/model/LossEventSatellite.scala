package com.gdp.domain.model

import java.sql.{Date, Timestamp};

case class LossEventSatellite(bk: String, cim_src_sys_id: String, cim_oe_id: String, cim_invalid_ts: Timestamp,
                              biz_eff_from_ts: Timestamp, cim_last_modification_ts: Timestamp, cim_load_from_ts: Timestamp,
                              loss_event_identifier: String, catastrophe_flag: String, lc_loss_event_status_cd: String,
                              loss_event_date_from: Date, loss_event_date_to: Date,
                              loss_event_name: String, lc_loss_event_cause_cd: String) extends Satellite {
}

object LossEventSatellite {
  val tableName: String = "loss_event_sat"
}