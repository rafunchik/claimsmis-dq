package com.gdp.ports.storage

import org.apache.spark.sql._
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import java.net.URI

class DefaultSparkStorageSpec extends AnyFlatSpec with Matchers with MockitoSugar with DefaultSparkStorageFixtures {


  "loadDataFrame" should "be able to load a dataframe from an URI" in {
    implicit val sparkSession: SparkSession = mock[SparkSession]
    withDefaultSparkStorage { sparkStorage =>
      withMockedDataFrameReader { dataFrameReader =>
        val dataFrame = mock[DataFrame]
        when(dataFrameReader.load(any[String]())).thenReturn(dataFrame)
        val uri = new URI("s3://bucket/path/")
        sparkStorage.loadDataFrame(uri) shouldBe dataFrame
      }
    }
  }


  it should "configure the reader with the options and the format" in {
    implicit val sparkSession: SparkSession = mock[SparkSession]
    withDefaultSparkStorage { sparkStorage =>
      withMockedDataFrameReader { dataFrameReader =>
        val options = Map("o1" -> "v1", "o2" -> "v2")
        val uri = new URI("s3://bucket/path/")
        val format = SparkDataFormat.Json
        sparkStorage.loadDataFrame(uri, options, format)
        verify(dataFrameReader).options(options)
        verify(dataFrameReader).format(SparkDataFormat.Json.source)
      }
    }
  }

}

trait DefaultSparkStorageFixtures extends MockitoSugar {
  def withDefaultSparkStorage(testBlock: DefaultSparkStorage => Any)
                             (implicit sparkSession: SparkSession): Any = {
    val sparkStorage = new DefaultSparkStorage()

    testBlock(sparkStorage)
  }

  def withMockedDataFrameReader(testBlock: DataFrameReader => Any)
                               (implicit sparkSession: SparkSession): Any = {

    val dataFrameReaderMock = mock[DataFrameReader]
    when(dataFrameReaderMock.options(any[Map[String, String]]())).thenReturn(dataFrameReaderMock)
    when(dataFrameReaderMock.format(any[String]())).thenReturn(dataFrameReaderMock)

    when(sparkSession.read).thenReturn(dataFrameReaderMock)

    testBlock(dataFrameReaderMock)
  }
}
