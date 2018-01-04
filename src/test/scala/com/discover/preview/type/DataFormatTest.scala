package com.discover.preview.`type`

import org.junit.Assert.assertEquals
import org.junit.Test

class DataFormatTest {


  @Test
  def testJson(): Unit = {

    val valid = DataFormat.fromString("json")
    assertEquals(valid, DataFormat.JSON)

  }

  @Test
  def testUnknown(): Unit = {

    val valid = DataFormat.fromString(".json")
    assertEquals(valid, DataFormat.UNKNOWN)

  }

  @Test
  def testEmpty(): Unit = {

    val valid = DataFormat.fromString("")
    assertEquals(valid, DataFormat.UNKNOWN)

  }

  @Test
  def testNone(): Unit = {

    val valid = DataFormat.fromString(null)
    assertEquals(valid, DataFormat.UNKNOWN)

  }


}
