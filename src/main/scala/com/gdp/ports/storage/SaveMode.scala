package com.gdp.ports.storage

sealed trait SaveMode
case object ErrorIfExists extends SaveMode
case object OverwriteOnlyIfNotComplete extends SaveMode
case object OverwriteAlways extends SaveMode
