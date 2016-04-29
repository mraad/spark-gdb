package com.esri.gdb

import java.nio.ByteBuffer

import com.esri.udt.{PointMType, PointMUDT}
import org.apache.spark.sql.types.Metadata

/**
  */
object FieldPointMType extends Serializable {
  def apply(name: String,
            nullValueAllowed: Boolean,
            xOrig: Double,
            yOrig: Double,
            mOrig: Double,
            xyScale: Double,
            mScale: Double,
            metadata: Metadata
           ) = {
    new FieldPointMType(name, nullValueAllowed, xOrig, yOrig, mOrig, xyScale, mScale, metadata)
  }
}

class FieldPointMType(name: String,
                      nullValueAllowed: Boolean,
                      xOrig: Double,
                      yOrig: Double,
                      mOrig: Double,
                      xyScale: Double,
                      mScale: Double,
                      metadata: Metadata)
  extends FieldBytes(name, new PointMUDT(), nullValueAllowed, metadata) {

  override def readValue(byteBuffer: ByteBuffer, oid: Int) = {
    val blob = getByteBuffer(byteBuffer)

    val geomType = blob.getVarUInt()

    val vx = blob.getVarUInt
    val vy = blob.getVarUInt
    val vm = blob.getVarUInt

    val x = (vx - 1.0) / xyScale + xOrig
    val y = (vy - 1.0) / xyScale + yOrig
    val m = (vm - 1.0) / mScale + mOrig

    new PointMType(x, y, m)
  }
}