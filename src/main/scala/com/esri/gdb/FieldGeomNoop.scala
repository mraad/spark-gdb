package com.esri.gdb

import java.nio.ByteBuffer

import org.apache.spark.sql.types.{BinaryType, Metadata}

/**
  */
class FieldGeomNoop(name: String, nullValueAllowed: Boolean)
  extends FieldBytes(name, BinaryType, nullValueAllowed, Metadata.empty) {

  override def readValue(byteBuffer: ByteBuffer, oid: Int) = {
    throw new RuntimeException("Should not have a NOOP geometry !")
  }

}
