package com.esri.gdb

import java.nio.ByteBuffer

import com.esri.udt.{PointType, PointUDT}
import org.apache.spark.sql.types.Metadata

object FieldPointType extends Serializable {
  def apply(name: String,
            nullValueAllowed: Boolean,
            xOrig: Double,
            yOrig: Double,
            xyScale: Double,
            metadata: Metadata) = {
    new FieldPointType(name, nullValueAllowed, xOrig, yOrig, xyScale, metadata)
  }
}

class FieldPointType(name: String,
                     nullValueAllowed: Boolean,
                     xOrig: Double,
                     yOrig: Double,
                     xyScale: Double,
                     metadata: Metadata)
  extends FieldBytes(name, new PointUDT(), nullValueAllowed, metadata) {

  override def readValue(byteBuffer: ByteBuffer, oid: Int) = {
    val blob = getByteBuffer(byteBuffer)

    blob.getVarUInt() // geomType

    val vx = blob.getVarUInt()
    val vy = blob.getVarUInt()
    val x = (vx - 1.0) / xyScale + xOrig
    val y = (vy - 1.0) / xyScale + yOrig

    new PointType(x, y)
  }
}