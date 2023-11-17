package com.gdp.domain.model

import java.sql.Timestamp;

trait CimEntity {
    val cim_src_sys_id: String
    val cim_oe_id: String
    val cim_invalid_ts: Timestamp
}
