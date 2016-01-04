package com.esri.gdb

import java.nio.ByteBuffer

import org.apache.spark.sql.types.{Metadata, StringType}

/**
  */
class FieldString(name: String, nullValueAllowed: Boolean, metadata: Metadata)
  extends FieldBytes(name, StringType, nullValueAllowed, metadata) {

  override def readValue(byteBuffer: ByteBuffer, oid: Int) = {
    val numBytes = fillVarBytes(byteBuffer)
    new String(m_bytes, 0, numBytes) // TODO - define Charset, like UTF-8 ?
  }

}
