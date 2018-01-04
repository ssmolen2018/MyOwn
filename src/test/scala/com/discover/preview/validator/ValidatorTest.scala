package com.discover.preview.validator

import com.discover.preview.`type`.DataFormat
import com.discover.preview.exception.InvalidFileTypeException
import org.junit.Assert._
import org.junit._

class ValidatorTest {

  @Test
  def testValid(): Unit = {

    val valid = ValidateInputFileFormat.getFileExtension("\\User\\local\\text.json")
    assertEquals(valid, DataFormat.JSON)

  }

  @Test (expected=classOf[ InvalidFileTypeException])
  def testInValid(): Unit = {

    val valid = ValidateInputFileFormat.getFileExtension("\\User\\local\\text.txt")

  }


  @Test (expected=classOf[ InvalidFileTypeException])
  def testInValidNoType(): Unit = {

    val valid = ValidateInputFileFormat.getFileExtension("\\User\\local\\text")

  }

  @Test (expected=classOf[ InvalidFileTypeException])
  def testInValidEmptyInput(): Unit = {

    val valid = ValidateInputFileFormat.getFileExtension("")

  }

  @Test
  def testValidParquet(): Unit = {

    val valid = ValidateInputFileFormat.getFileExtension("\\User\\local\\text.parquet")
    assertEquals(valid, DataFormat.PARQUET)

  }

  @Test (expected=classOf[ InvalidFileTypeException])
  def testNull(): Unit = {

    val valid = ValidateInputFileFormat.getFileExtension(null)

  }

}