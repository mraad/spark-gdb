package com.esri.gdb

import java.nio.{ByteBuffer, ByteOrder}

import org.apache.hadoop.fs.FSDataInputStream

/**
  */
class DataBuffer(dataInput: FSDataInputStream) extends Serializable {

  private var bytes = new Array[Byte](1024)
  private var byteBuffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)

  def readBytes(length: Int) = {
    if (length > bytes.length) {
      bytes = new Array[Byte](length)
      byteBuffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
    }
    else {
      byteBuffer.clear
    }
    dataInput.readFully(bytes, 0, length)
    byteBuffer
  }

  def seek(position: Long) = {
    dataInput.seek(position)
    this
  }

  def close() {
    dataInput.close()
  }
}

object DataBuffer {
  def apply(dataInput: FSDataInputStream) = {
    new DataBuffer(dataInput)
  }
}
