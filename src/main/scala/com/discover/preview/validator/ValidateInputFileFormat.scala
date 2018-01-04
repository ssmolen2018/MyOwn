package com.discover.preview.validator

import com.discover.preview.`type`.DataFormat
import com.discover.preview.`type`.DataFormat.DataFormat
import com.discover.preview.exception.InvalidFileTypeException
import com.discover.preview.service.{JsonFileReader, ParquetFileReader}
import com.discover.preview.util.StringUtil

object ValidateInputFileFormat {

  private val ext = """\.[A-Za-z0-9]+$""".r

  def getFileExtension(fileName:String) : DataFormat = {

    var fileExtension:String = (ext findFirstIn StringUtil.getEmpty(fileName)).getOrElse("")

    val fileType = DataFormat.fromString((fileExtension.replaceAll("^\\.", "")))

      fileType match {
      case DataFormat.UNKNOWN =>
        throw new InvalidFileTypeException(fileName)
      case _ => fileType

    }

    fileType

  }

  def getFileExtensionString(fileName:String) : String = {

    getFileExtension(fileName).toString


  }





}
