package com.esri.gdb

import java.nio.ByteBuffer

import com.esri.udt.{PointType, PointUDT}
import org.apache.spark.sql.types.{DataType, Metadata}

object FieldPoint {
  def apply(name: String,
            nullValueAllowed: Boolean,
            xOrig: Double,
            yOrig: Double,
            xyScale: Double,
            xyTolerance: Double,
            metadata: Metadata) = {
    new FieldPointEsri(name, nullValueAllowed, xOrig, yOrig, xyScale, xyTolerance, metadata)
  }
}

abstract class FieldPoint(name: String,
                          dataType: DataType,
                          nullValueAllowed: Boolean,
                          xOrig: Double,
                          yOrig: Double,
                          xyScale: Double,
                          xyTolerance: Double,
                          metadata: Metadata
                         ) extends FieldGeom(name, dataType, nullValueAllowed, xOrig, yOrig, xyScale, metadata) {

  override def readValue(byteBuffer: ByteBuffer, oid: Int) = {
    val blob = getByteBuffer(byteBuffer)

    val geomType = blob getVarUInt

    val vx = blob getVarUInt
    val vy = blob getVarUInt
    val x = (vx - 1.0) / xyScale + xOrig
    val y = (vy - 1.0) / xyScale + yOrig

    new PointType(x, y)
  }
}


class FieldPointEsri(name: String,
                     nullValueAllowed: Boolean,
                     xOrig: Double,
                     yOrig: Double,
                     xyScale: Double,
                     xyTolerance: Double,
                     metadata: Metadata
                    )
  extends FieldPoint(name, new PointUDT(), nullValueAllowed, xOrig, yOrig, xyScale, xyTolerance, metadata)
