package com.esri.gdb

import java.nio.ByteBuffer

import org.apache.spark.sql.types.{DoubleType, Metadata}

/**
  */
class FieldFloat64(name: String, nullValueAllowed: Boolean, metadata: Metadata)
  extends Field(name, DoubleType, nullValueAllowed, metadata) {

  override def readValue(byteBuffer: ByteBuffer, oid: Int) = {
    byteBuffer.getDouble
  }
}
