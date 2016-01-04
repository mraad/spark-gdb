package com.esri.gdb

import scala.collection.mutable

/**
  */
class GDBTableSeekWithNullValues(dataBuffer: DataBuffer,
                                 fields: Seq[Field],
                                 numFieldsWithNullAllowed: Int,
                                 indexIter: Iterator[IndexInfo])
  extends Iterator[Map[String, Any]] with Serializable {

  private val nullValueIndicators = new Array[Byte]((numFieldsWithNullAllowed / 8.0).ceil.toInt)

  def hasNext() = indexIter.hasNext

  def next() = {
    val index = indexIter.next()
    val numBytes = dataBuffer.seek(index.seek).readBytes(4).getInt
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
          map(field.name) = field.readValue(byteBuffer, index.objectID)
        }
      } else {
        map(field.name) = field.readValue(byteBuffer, index.objectID)
      }
    }
    )
    map.toMap
  }
}
