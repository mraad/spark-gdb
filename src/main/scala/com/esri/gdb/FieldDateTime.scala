package com.esri.gdb

import java.nio.ByteBuffer
import java.sql.Timestamp

import org.apache.spark.sql.types.{Metadata, TimestampType}

/**
  */
class FieldDateTime(name: String, nullValueAllowed: Boolean, metadata:Metadata)
  extends Field(name, TimestampType, nullValueAllowed, metadata) {

  override def readValue(byteBuffer: ByteBuffer, oid: Int) = {
    val numDays = byteBuffer.getDouble
    // convert days since 12/30/1899 to 1/1/1970
    val unixDays = numDays - 25569
    val millis = (unixDays * 1000 * 60 * 60 * 24).ceil.toLong
    new Timestamp(millis)
  }
}
