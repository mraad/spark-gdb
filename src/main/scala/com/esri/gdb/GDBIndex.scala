package com.esri.gdb

import java.io.{DataInput, File}
import java.nio.{ByteBuffer, ByteOrder}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, Path}

object GDBIndex {
  def apply(path: String, name: String, conf: Configuration = new Configuration()) = {
    val filename = StringBuilder.newBuilder.append(path).append(File.separator).append(name).append(".gdbtablx").toString()
    val hdfsPath = new Path(filename)
    val dataInput = hdfsPath.getFileSystem(conf).open(hdfsPath)

    val bytes = new Array[Byte](16)
    dataInput.readFully(bytes)
    val byteBuffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)

    val signature = byteBuffer.getInt
    val n1024Blocks = byteBuffer.getInt
    val numRows = byteBuffer.getInt
    val indexSize = byteBuffer.getInt

    new GDBIndex(dataInput, numRows, indexSize)
  }
}

private[gdb] class GDBIndex(dataInput: FSDataInputStream,
                            val numRows: Int,
                            indexSize: Int
                           ) extends AutoCloseable with Serializable {

  def readSeekForRowNum(rowNum: Int) = {
    val bytes = new Array[Byte](indexSize)
    dataInput.seek(16 + rowNum * indexSize)
    dataInput.readFully(bytes)
    ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getInt
  }

  def iterator(startAtRow: Int = 0, numRowsToRead: Int = -1) = {
    dataInput.seek(16 + startAtRow * indexSize)
    val maxRows = if (numRowsToRead == -1) numRows else numRowsToRead
    new GDBIndexIterator(dataInput, startAtRow, maxRows, indexSize).withFilter(_.isSeekable)
  }

  def close() {
    dataInput.close()
  }
}

private[gdb] class GDBIndexIterator(dataInput: DataInput,
                                    startID: Int,
                                    maxRows: Int,
                                    indexSize: Int
                                   ) extends Iterator[IndexInfo] with Serializable {

  private val indexInfo = IndexInfo(0, 0)
  private val bytes = new Array[Byte](indexSize)
  private val byteBuffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)

  private var objectID = startID
  private var nextRow = 0

  def hasNext() = nextRow < maxRows

  def next() = {
    nextRow += 1

    objectID += 1
    indexInfo.objectID = objectID

    byteBuffer.clear
    dataInput.readFully(bytes)
    indexInfo.seek = byteBuffer.getInt

    indexInfo
  }
}
