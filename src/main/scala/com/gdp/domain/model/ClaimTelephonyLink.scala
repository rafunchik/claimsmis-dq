package com.gdp.domain.model

import java.sql.Timestamp;

case class ClaimTelephonyLink(cim_src_sys_id: String, cim_oe_id: String, cim_invalid_ts: Timestamp, bk_claim: String, bk_telephony: String) extends CimEntity {
}

object ClaimTelephonyLink {
  val tableName: String = "telephony_lnk"
}