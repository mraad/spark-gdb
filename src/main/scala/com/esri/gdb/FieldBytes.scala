package com.esri.gdb

import java.nio.ByteBuffer

import org.apache.spark.sql.types.{DataType, Metadata}

/**
  */
abstract class FieldBytes(name: String,
                          dataType: DataType,
                          nullValueAllowed: Boolean,
                          metadata: Metadata = Metadata.empty
                         )
  extends Field(name, dataType, nullValueAllowed, metadata) {

  protected var m_bytes = new Array[Byte](1024)

  def getByteBuffer(byteBuffer: ByteBuffer) = {
    val numBytes = fillVarBytes(byteBuffer)
    ByteBuffer.wrap(m_bytes, 0, numBytes)
  }

  def fillVarBytes(byteBuffer: ByteBuffer) = {
    val numBytes = byteBuffer.getVarUInt.toInt
    if (numBytes > m_bytes.length) {
      m_bytes = new Array[Byte](numBytes)
    }
    0 until numBytes foreach {
      m_bytes(_) = byteBuffer.get
    }
    numBytes
  }
}
