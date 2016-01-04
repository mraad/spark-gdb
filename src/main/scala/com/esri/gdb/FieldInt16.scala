package com.esri.gdb

import java.nio.ByteBuffer

import org.apache.spark.sql.types.{Metadata, ShortType}

/**
  */
class FieldInt16(name: String, nullValueAllowed: Boolean, metadata: Metadata)
  extends Field(name, ShortType, nullValueAllowed, metadata) {

  override def readValue(byteBuffer: ByteBuffer, oid: Int) = {
    byteBuffer.getShort
  }
}
