package com.esri.gdb

import java.nio.ByteBuffer

import org.apache.spark.sql.types.{BinaryType, Metadata}

/**
  */
class FieldGeomNoop(name: String, nullValueAllowed: Boolean)
  extends FieldGeom(name, BinaryType, nullValueAllowed, 0.0, 0.0, 1.0, Metadata.empty) {

  override def readValue(byteBuffer: ByteBuffer, oid: Int) = {
    throw new RuntimeException("Should not have a NOOP geometry !")
  }

}
