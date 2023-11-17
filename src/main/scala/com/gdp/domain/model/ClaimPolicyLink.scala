package com.gdp.domain.model

import java.sql.Timestamp;

case class ClaimPolicyLink(cim_src_sys_id: String, cim_oe_id: String, cim_invalid_ts: Timestamp, bk_claim: String, bk_policy: String) extends CimEntity {
}
object ClaimPolicyLink {
  val tableName: String = "policy_lnk"
}