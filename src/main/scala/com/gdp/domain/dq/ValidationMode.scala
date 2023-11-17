package com.gdp.domain.dq

sealed trait ValidationMode
case object RejectDataset extends ValidationMode
case object SkipRow extends ValidationMode
case object ReportOnly extends ValidationMode
