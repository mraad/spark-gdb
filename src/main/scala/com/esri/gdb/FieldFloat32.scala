package com.esri.gdb

import java.nio.ByteBuffer

import org.apache.spark.sql.types.{FloatType, Metadata}

/**
  */
class FieldFloat32(name: String, nullValueAllowed: Boolean, metadata: Metadata)
  extends Field(name, FloatType, nullValueAllowed, metadata) {

  override def readValue(byteBuffer: ByteBuffer, oid: Int) = {
    byteBuffer.getFloat
  }
}
