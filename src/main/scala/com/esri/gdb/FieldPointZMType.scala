package com.esri.gdb

import java.nio.ByteBuffer

import com.esri.udt.{PointZMType, PointZMUDT}
import org.apache.spark.sql.types.Metadata

/**
  */
object FieldPointZMType extends Serializable {
  def apply(name: String,
            nullValueAllowed: Boolean,
            xOrig: Double,
            yOrig: Double,
            zOrig: Double,
            mOrig: Double,
            xyScale: Double,
            zScale: Double,
            mScale: Double,
            metadata: Metadata
           ) = {
    new FieldPointZMType(name, nullValueAllowed, xOrig, yOrig, zOrig, mOrig, xyScale, zScale, mScale, metadata)
  }
}

class FieldPointZMType(name: String,
                       nullValueAllowed: Boolean,
                       xOrig: Double,
                       yOrig: Double,
                       zOrig: Double,
                       mOrig: Double,
                       xyScale: Double,
                       zScale: Double,
                       mScale: Double,
                       metadata: Metadata)
  extends FieldBytes(name, new PointZMUDT(), nullValueAllowed, metadata) {

  override def readValue(byteBuffer: ByteBuffer, oid: Int) = {
    val blob = getByteBuffer(byteBuffer)

    val geomType = blob.getVarUInt

    val vx = blob.getVarUInt()
    val vy = blob.getVarUInt()
    val x = (vx - 1.0) / xyScale + xOrig
    val y = (vy - 1.0) / xyScale + yOrig

    geomType match {
      // Point
      case 1 => new PointZMType(x, y)
      // PointZ
      case 9 =>
        val vz = blob.getVarUInt
        val z = (vz - 1.0) / zScale + zOrig
        new PointZMType(x, y, z)
      // PointM
      case 21 =>
        val vm = blob.getVarUInt
        val m = (vm - 1.0) / mScale + mOrig
        new PointZMType(x, y, 0.0, m)
      // PointZM
      case _ =>
        val vz = blob.getVarUInt
        val vm = blob.getVarUInt
        val z = (vz - 1.0) / zScale + zOrig
        val m = (vm - 1.0) / mScale + mOrig
        new PointZMType(x, y, z, m)
    }
  }
}