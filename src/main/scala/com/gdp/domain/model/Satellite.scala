package com.gdp.domain.model

import java.sql.Timestamp;

trait Satellite extends CimEntity {
    val biz_eff_from_ts: Timestamp
    val cim_load_from_ts: Timestamp
    val cim_last_modification_ts: Timestamp
}
