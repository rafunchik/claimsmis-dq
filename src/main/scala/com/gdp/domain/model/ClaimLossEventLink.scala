package com.gdp.domain.model

import java.sql.Timestamp;

case class ClaimLossEventLink(cim_src_sys_id: String, cim_oe_id: String, cim_invalid_ts: Timestamp, bk_claim: String, bk_loss_event: String) extends CimEntity


object ClaimLossEventLink {
    val tableName: String = "loss_event_lnk"
}