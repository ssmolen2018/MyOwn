package com.discover.preview.service

import com.discover.preview.`trait`.Reader
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

/**
  * Json File reader implementation of the Reader.
  */
case class JsonFileReader(sparkSession: SparkSession) extends Reader {

  override def read(
                     url:String) : DataFrame = {

         sparkSession.read.json(url)
  }

}


