package com.discover.preview.service

import com.discover.preview.`trait`.Reader
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}


case class ParquetFileReader(sparkSession: SparkSession) extends Reader {

  override def read(url:String) : DataFrame = {

    sparkSession.read.parquet(url)
  }

}


