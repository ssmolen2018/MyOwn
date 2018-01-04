package com.discover.preview.util

object StringUtil {

  private val BLANK:String = ""

  def isEmpty(x: String) = x == null || x.trim.isEmpty

  def getEmpty(s: String): String = {
    if (isEmpty(s))
      BLANK
    else
      s

  }

}
