package com.esri.gdb

/**
  */
class GDBTableSeekWithNoNullValues(dataBuffer: DataBuffer, fields: Seq[Field], indexIter: Iterator[IndexInfo])
  extends Iterator[Map[String, Any]] with Serializable {

  def hasNext() = indexIter.hasNext

  def next() = {
    val index = indexIter.next()
    val numBytes = dataBuffer.seek(index.seek).readBytes(4).getInt
    val byteBuffer = dataBuffer.readBytes(numBytes)
    fields.map(_.readTuple(byteBuffer, index.objectID)).toMap
  }
}
