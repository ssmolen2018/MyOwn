package com.discover.preview.`trait`

import org.apache.spark.sql.DataFrame

// All concrete readers will extend this trait
trait Reader {
  def read(fileUrl: String): DataFrame
}