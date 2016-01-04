package com.esri.gdb

import scala.collection.mutable

/**
  */
class GDBTableScanWithNullValues(dataBuffer: DataBuffer, fields: Seq[Field], maxRows: Int, startID: Int = 0)
  extends Iterator[Map[String, Any]] with Serializable {

  val numFieldsWithNullAllowed = fields.count(_.nullable)
  val nullValueIndicators = new Array[Byte]((numFieldsWithNullAllowed / 8.0).ceil.toInt)

  var nextRow = 0
  var objectID = startID

  def hasNext() = nextRow < maxRows

  def next() = {
    nextRow += 1
    objectID += 1
    val numBytes = dataBuffer.readBytes(4).getInt
    val byteBuffer = dataBuffer.readBytes(numBytes)
    0 until nullValueIndicators.length foreach (nullValueIndicators(_) = byteBuffer.get)
    var bit = 0
    val map = mutable.Map[String, Any]()
    fields.foreach(field => {
      if (field.nullable) {
        val i = bit >> 3
        val m = 1 << (bit & 7)
        bit += 1
        if ((nullValueIndicators(i) & m) == 0) {
          map(field.name) = field.readValue(byteBuffer, objectID)
        }
      } else {
        map(field.name) = field.readValue(byteBuffer, objectID)
      }
    }
    )
    map.toMap
  }
}
