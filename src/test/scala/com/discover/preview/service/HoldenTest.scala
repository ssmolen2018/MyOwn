package com.discover.preview.service


import com.discover.preview.`trait`.Reader
import com.discover.preview.`type`.DataFormat
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterEach, FunSuite, Matchers}


class HoldenTest  extends FunSuite with Matchers with BeforeAndAfterEach {
/*
  private val master = "local[*]"
  private val appName = "data_load_testing"

  var spark: SparkSession = _

  override def beforeEach() {
    spark = new SparkSession.Builder().appName(appName).master(master).getOrCreate()
  }

  override def afterEach() {
    spark.stop()
  }

  test("Passing DataFormat should return corresponding Reader") {

    var reader:Reader = ReaderFactory(spark, DataFormat.PARQUET)
    assert(reader.isInstanceOf[ParquetFileReader])

    reader = ReaderFactory(spark, DataFormat.JSON)
    assert(reader.isInstanceOf[JsonFileReader])

  }

*/


}
