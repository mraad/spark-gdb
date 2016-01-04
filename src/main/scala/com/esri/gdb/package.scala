package com.esri

import java.nio.ByteBuffer

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  */
package object gdb {

  implicit class ByteBufferImplicits(byteBuffer: ByteBuffer) {

    implicit def getVarUInt() = {
      var shift = 7
      var b: Long = byteBuffer.get
      var ret = b & 0x7FL
      var old = ret
      while ((b & 0x80L) != 0) {
        b = byteBuffer.get
        ret = ((b & 0x7FL) << shift) | old
        old = ret
        shift += 7
      }
      ret
    }

    implicit def getVarInt() = {
      var shift = 7
      var b: Long = byteBuffer.get
      val isNeg = (b & 0x40L) != 0
      var ret = b & 0x3FL
      var old = ret
      while ((b & 0x80L) != 0) {
        b = byteBuffer.get
        ret = ((b & 0x7FL) << (shift - 1)) | old
        old = ret
        shift += 7
      }
      if (isNeg) -ret else ret
    }
  }

  implicit class SparkContextImplicits(sc: SparkContext) {
    implicit def gdbFile(path: String, name: String, serde: String = null, numPartitions: Int = 8) = {
      GDBRDD(sc, path, name, serde, numPartitions)
    }
  }

  implicit class SQLContextImplicits(sqlContext: SQLContext) extends Serializable {
    implicit def gdbFile(path: String, name: String, serde: String = null, numPartitions: Int = 8) = {
      sqlContext.baseRelationToDataFrame(GDBRelation(path, name, serde, numPartitions)(sqlContext))
    }
  }

}
