package com.esri.gdb

import java.nio.ByteBuffer

import org.apache.spark.sql.types.{IntegerType, Metadata}

/**
  */
class FieldOID(name: String, nullValueAllowed: Boolean, metadata: Metadata)
  extends Field(name, IntegerType, nullValueAllowed, metadata) {

  override def readValue(byteBuffer: ByteBuffer, oid: Int) = {
    oid
  }
}
