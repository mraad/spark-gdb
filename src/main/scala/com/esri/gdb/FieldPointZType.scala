package com.esri.gdb

import java.nio.ByteBuffer

import com.esri.udt.{PointZType, PointZUDT}
import org.apache.spark.sql.types.Metadata

/**
  */
object FieldPointZType extends Serializable {
  def apply(name: String,
            nullValueAllowed: Boolean,
            xOrig: Double,
            yOrig: Double,
            zOrig: Double,
            xyScale: Double,
            zScale: Double,
            metadata: Metadata
           ) = {
    new FieldPointZType(name, nullValueAllowed, xOrig, yOrig, zOrig, xyScale, zScale, metadata)
  }
}

class FieldPointZType(name: String,
                      nullValueAllowed: Boolean,
                      xOrig: Double,
                      yOrig: Double,
                      zOrig: Double,
                      xyScale: Double,
                      zScale: Double,
                      metadata: Metadata)
  extends FieldBytes(name, new PointZUDT(), nullValueAllowed, metadata) {

  override def readValue(byteBuffer: ByteBuffer, oid: Int) = {
    val blob = getByteBuffer(byteBuffer)

    val geomType = blob.getVarUInt

    val vx = blob.getVarUInt
    val vy = blob.getVarUInt
    val vz = blob.getVarUInt

    val x = (vx - 1.0) / xyScale + xOrig
    val y = (vy - 1.0) / xyScale + yOrig
    val z = (vz - 1.0) / zScale + zOrig

    new PointZType(x, y, z)
  }
}