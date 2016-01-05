package com.esri.gdb

import java.nio.ByteBuffer

import org.apache.spark.sql.types.{DataType, Metadata, StructField}

/**
  */
abstract class Field(name: String,
                     dataType: DataType,
                     nullValueAllowed: Boolean,
                     metadata: Metadata = Metadata.empty
                    ) extends StructField(name, dataType, nullValueAllowed, metadata) {

  def readValue(byteBuffer: ByteBuffer, oid: Int): Any

  def readTuple(byteBuffer: ByteBuffer, oid: Int) = {
    name -> readValue(byteBuffer, oid)
  }
}
