package com.gdp.domain.model

import java.sql.Timestamp

case class LossEventHub(bk: String, cim_src_sys_id: String, cim_oe_id: String, cim_invalid_ts: Timestamp) extends Hub {
}

object LossEventHub {
  val tableName: String = "loss_event_hub"
}