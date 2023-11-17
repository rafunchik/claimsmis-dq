package com.gdp.domain.dq

import org.apache.spark.sql.SparkSession

/**
 * To be mixed with Tests so they can use a default spark context suitable for testing
 */
trait SparkContextSpec {

  /**
   * @param testFun thunk to run with SparkSession as an argument
   */
  def withSparkSession(testFun: SparkSession => Any): Unit = {

    val session = setupSparkSession
    try {
      testFun(session)
    } finally {
      /* empty cache of RDD size, as the referred ids are only valid within a session */
      tearDownSparkSession(session)
    }
  }

  /**
   * Setups a local sparkSession
   *
   * @return sparkSession to be used
   */
  private def setupSparkSession = {
    val session = SparkSession.builder()
      .master("local")
      .appName("test")
      .config( "spark.driver.host", "localhost" )
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", 2.toString)
      .config("spark.sql.adaptive.enabled", value = false)
      .getOrCreate()
    session.sparkContext.setCheckpointDir(System.getProperty("java.io.tmpdir"))
    session
  }

  /**
   * Tears down the sparkSession
   *
   * @param session Session to be stopped
   * @return
   */
  private def tearDownSparkSession(session: SparkSession) = {
    session.stop()
    System.clearProperty("spark.driver.port")
  }

}
