package com.esri.gdb

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType

/**
  */
class GDBRowIterator(indexIter: Iterator[IndexInfo], dataBuffer: DataBuffer, fields: Array[Field], schema: StructType)
  extends Iterator[Row] with Serializable {

  val numFieldsWithNullAllowed = fields.count(_.nullable)
  val nullValueMasks = new Array[Byte]((numFieldsWithNullAllowed / 8.0).ceil.toInt)

  def hasNext() = indexIter.hasNext

  def next() = {
    val index = indexIter.next()
    val numBytes = dataBuffer.seek(index.seek).readBytes(4).getInt
    val byteBuffer = dataBuffer.readBytes(numBytes)
    0 until nullValueMasks.length foreach (nullValueMasks(_) = byteBuffer.get)
    var bit = 0
    val values = fields.map(field => {
      if (field.nullable) {
        val i = bit >> 3
        val m = 1 << (bit & 7)
        bit += 1
        if ((nullValueMasks(i) & m) == 0) {
          field.readValue(byteBuffer, index.objectID)
        }
        else {
          null // TODO - Do not like null here - but...it is nullable !
        }
      } else {
        field.readValue(byteBuffer, index.objectID)
      }
    }
    )
    new GenericRowWithSchema(values, schema)
  }
}
