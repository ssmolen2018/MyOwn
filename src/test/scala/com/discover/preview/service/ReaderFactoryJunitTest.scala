package com.discover.preview.service

import com.discover.preview.`trait`.Reader
import com.discover.preview.`type`.DataFormat
import com.discover.preview.exception.InvalidDataTypeException
import org.apache.spark.sql.SparkSession
import org.junit.{After, Before, Test}


class ReaderFactoryJunitTest  {

  private val master = "local[*]"
  private val appName = "data_load_testing"

  var spark: SparkSession = new SparkSession.Builder().appName(appName).master(master).getOrCreate()

  @After
  def afterEach() {
    if (spark != null)
       spark.stop()
  }

  @Test
  def test(): Unit =
   {

    var reader:Reader = ReaderFactory(spark, DataFormat.PARQUET)
    assert(reader.isInstanceOf[ParquetFileReader])

    reader = ReaderFactory(spark, DataFormat.JSON)
    assert(reader.isInstanceOf[JsonFileReader])

  }

  @Test (expected=classOf[ InvalidDataTypeException])
  def testInvalid(): Unit =
  {

    var reader:Reader = ReaderFactory(spark, DataFormat.UNKNOWN)

  }




}
