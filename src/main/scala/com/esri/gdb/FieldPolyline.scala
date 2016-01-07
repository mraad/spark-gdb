package com.esri.gdb

import java.nio.ByteBuffer

import com.esri.core.geometry.Polyline
import com.esri.udt.ShapeEsri
import org.apache.spark.sql.types.{DataType, Metadata}

object FieldPolyline extends Serializable {
  def apply(name: String,
            nullValueAllowed: Boolean,
            xOrig: Double,
            yOrig: Double,
            xyScale: Double,
            metadata: Metadata) = {
    new FieldPolylineEsri(name, nullValueAllowed, xOrig, yOrig, xyScale, metadata)
  }
}

abstract class FieldPolyline(name: String,
                             dataType: DataType,
                             nullValueAllowed: Boolean,
                             xOrig: Double,
                             yOrig: Double,
                             xyScale: Double,
                             metadata: Metadata
                            )
  extends FieldPoly(name, dataType, nullValueAllowed, xOrig, yOrig, xyScale, metadata) {

  override def readValue(byteBuffer: ByteBuffer, oid: Int) = {
    val polyline = new Polyline()

    val blob = getByteBuffer(byteBuffer)
    val geomType = blob getVarUInt

    val numPoints = blob.getVarUInt.toInt
    val numParts = blob.getVarUInt.toInt

    val xmin = blob.getVarUInt / xyScale + xOrig
    val ymin = blob.getVarUInt / xyScale + yOrig
    val xmax = blob.getVarUInt / xyScale + xmin
    val ymax = blob.getVarUInt / xyScale + ymin

    dx = 0L
    dy = 0L

    if (numParts > 1) {
      var sum = 0
      val numCoordSeq = 1 to numParts map (part => {
        val numCoord = if (part == numParts) {
          numPoints - sum
        } else {
          blob.getVarUInt.toInt
        }
        sum += numCoord
        numCoord
      })
      numCoordSeq.foreach(numCoord => addPath(blob, numCoord, polyline))
    }
    else {
      addPath(blob, numPoints, polyline)
    }
    polyline
  }
}

class FieldPolylineEsri(name: String,
                        nullValueAllowed: Boolean,
                        xOrig: Double,
                        yOrig: Double,
                        xyScale: Double,
                        metadata: Metadata
                       )
  extends FieldPolyline(name, ShapeEsri("polyline"), nullValueAllowed, xOrig, yOrig, xyScale, metadata)

