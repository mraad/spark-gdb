package com.esri.gdb

import java.nio.ByteBuffer

import com.esri.core.geometry.MultiPath
import org.apache.spark.sql.types.{DataType, Metadata}

abstract class FieldPoly(name: String,
                         dataType: DataType,
                         nullValueAllowed: Boolean,
                         xOrig: Double,
                         yOrig: Double,
                         xyScale: Double,
                         metadata: Metadata)
  extends FieldBytes(name, dataType, nullValueAllowed, metadata) {

  protected var dx = 0L
  protected var dy = 0L

  def addPath(byteBuffer: ByteBuffer, numCoordinates: Int, path: MultiPath) = {
    0 until numCoordinates foreach (n => {
      dx += byteBuffer.getVarInt
      dy += byteBuffer.getVarInt
      val x = dx / xyScale + xOrig
      val y = dy / xyScale + yOrig
      n match {
        case 0 => path.startPath(x, y)
        case _ => path.lineTo(x, y)
      }
    })
    path
  }
}
