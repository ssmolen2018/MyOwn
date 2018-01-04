package com.discover.preview.service

import com.discover.preview.`trait`.Reader
import com.discover.preview.`type`.DataFormat
import org.apache.spark.sql.SparkSession


/**
  * Return Reader based on conditions
  * Currently, only CSV reader is implemented.
  */
object ReaderFactory {
  def apply(sparkSession: SparkSession,dataType:DataFormat.Value):
  Reader   = {

    dataType match {
      case DataFormat.PARQUET =>
        ParquetFileReader(sparkSession)
      case _ => JsonFileReader(sparkSession)
    }
  }
}

