package com.esri.gdb

import java.nio.ByteBuffer

import com.vividsolutions.jts.geom.GeometryFactory
import org.apache.spark.sql.types.{Metadata, StringType}

/**
  */
class FieldUUID(name: String, nullValueAllowed: Boolean, metadata:Metadata)
  extends Field(name, StringType, nullValueAllowed, metadata) {

  private val b = new Array[Byte](16)

  override def readValue(byteBuffer: ByteBuffer, oid: Int) = {

    0 until 16 foreach (b(_) = byteBuffer.get)

    "{%02X%02X%02X%02X-%02X%02X-%02X%02X-%02X%02X-%02X%02X%02X%02X%02X%02X}".format(
      b(3), b(2), b(1), b(0),
      b(5), b(4), b(7), b(6),
      b(8), b(9), b(10), b(11),
      b(12), b(13), b(14), b(15))
  }
}
