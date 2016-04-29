package com.esri.gdb

import java.nio.ByteBuffer

import com.esri.core.geometry.Polygon
import com.esri.udt.PolygonUDT
import org.apache.spark.sql.types.{DataType, Metadata}

@deprecated("not used", "0.4")
object FieldPolygon {
  def apply(name: String,
            nullValueAllowed: Boolean,
            xOrig: Double,
            yOrig: Double,
            xyScale: Double,
            metadata: Metadata) = {
    new FieldPolygonEsri(name, nullValueAllowed, xOrig, yOrig, xyScale, metadata)
  }
}

@deprecated("not used", "0.4")
abstract class FieldPolygon(name: String,
                            dataType: DataType,
                            nullValueAllowed: Boolean,
                            xOrig: Double,
                            yOrig: Double,
                            xyScale: Double,
                            metadata: Metadata
                           )
  extends FieldPoly(name, dataType, nullValueAllowed, xOrig, yOrig, xyScale, metadata) {

  override def readValue(byteBuffer: ByteBuffer, oid: Int) = {
    val polygon = new Polygon()

    val blob = getByteBuffer(byteBuffer)

    val geomType = blob.getVarUInt

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
      // TODO - fix shells and holes based on https://github.com/rouault/dump_gdbtable/wiki/FGDB-Spec
      numCoordSeq.foreach(numCoord => addPath(blob, numCoord, polygon))
    }
    else {
      addPath(blob, numPoints, polygon)
    }
    polygon
  }
}

@deprecated("not used", "0.4")
class FieldPolygonEsri(name: String,
                       nullValueAllowed: Boolean,
                       xOrig: Double,
                       yOrig: Double,
                       xyScale: Double,
                       metadata: Metadata)
  extends FieldPolygon(name, new PolygonUDT(), nullValueAllowed, xOrig, yOrig, xyScale, metadata)
