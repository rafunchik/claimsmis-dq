package com.gdp.domain.model

import java.sql.Timestamp;

case class ClaimClaimPartLink(cim_src_sys_id: String, cim_oe_id: String, cim_invalid_ts: Timestamp, bk_claim: String, bk_claim_part: String) extends CimEntity {
}

object ClaimClaimPartLink {
    val tableName: String = "clm_part_lnk"
}
