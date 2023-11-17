package com.gdp.ports.report

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Date;

trait CheckResultWriter {
    def writeDQFailure(oe: String, source: String, bizzEffectiveDate: Date, results: DataFrame, numRows: Long)(implicit sparkSession: SparkSession): Unit
}
