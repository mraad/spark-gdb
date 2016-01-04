package com.esri.gdb

/**
  */
case class IndexInfo(var objectID: Int, var seek: Int) {
  def isSeekable = seek > 0
}
