package com.discover.preview.`type`

import com.discover.preview.util.StringUtil

/**
  * Supported data formats ( for read/write)
  */
object DataFormat extends Enumeration {

  type DataFormat = Value
  val JSON,PARQUET,AVRO,UNKNOWN = Value

  def fromString(name: String): Value =


    values.find(_.toString.toLowerCase == StringUtil.getEmpty(name).toLowerCase()).getOrElse(UNKNOWN)

}