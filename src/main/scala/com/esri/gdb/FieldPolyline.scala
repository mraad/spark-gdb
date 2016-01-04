package com.esri.gdb

import java.nio.ByteBuffer

import com.esri.udt.{ShapeJTS, ShapeWKB, ShapeWKT}
import com.vividsolutions.jts.geom.{GeometryFactory, PrecisionModel}
import org.apache.spark.sql.types.{DataType, Metadata}

object FieldPolyline {
  def apply(name: String,
            nullValueAllowed: Boolean,
            xOrig: Double,
            yOrig: Double,
            xyScale: Double,
            xyTolerance: Double,
            metadata: Metadata,
            serde: String) = {
    serde match {
      case "wkt" => new FieldPolylineWKT(name, nullValueAllowed, xOrig, yOrig, xyScale, xyTolerance, metadata)
      case "wkb" => new FieldPolylineWKB(name, nullValueAllowed, xOrig, yOrig, xyScale, xyTolerance, metadata)
      case _ => new FieldPolylineJTS(name, nullValueAllowed, xOrig, yOrig, xyScale, xyTolerance, metadata)
    }
  }
}

abstract class FieldPolyline(name: String,
                             dataType: DataType,
                             nullValueAllowed: Boolean,
                             xOrig: Double,
                             yOrig: Double,
                             xyScale: Double,
                             xyTolerance: Double,
                             metadata: Metadata
                            )
  extends FieldPoly(name, dataType, nullValueAllowed, xOrig, yOrig, xyScale, metadata) {

  @transient val geomFact = new GeometryFactory(new PrecisionModel(1.0 / xyTolerance))

  override def readValue(byteBuffer: ByteBuffer, oid: Int) = {
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
      geomFact.createMultiLineString(numCoordSeq.map(numCoord =>
        geomFact.createLineString(getCoordinates(blob, numCoord))
      ).toArray)
    }
    else {
      geomFact.createLineString(getCoordinates(blob, numPoints))
    }
  }
}

class FieldPolylineJTS(name: String,
                       nullValueAllowed: Boolean,
                       xOrig: Double,
                       yOrig: Double,
                       xyScale: Double,
                       xyTolerance: Double,
                       metadata: Metadata
                      ) extends FieldPolyline(name, ShapeJTS("polyline_jts"), nullValueAllowed, xOrig, yOrig, xyScale, xyTolerance, metadata)

class FieldPolylineWKT(name: String,
                       nullValueAllowed: Boolean,
                       xOrig: Double,
                       yOrig: Double,
                       xyScale: Double,
                       xyTolerance: Double,
                       metadata: Metadata
                      ) extends FieldPolyline(name, ShapeWKT("polyline_wkt", xyTolerance), nullValueAllowed, xOrig, yOrig, xyScale, xyTolerance, metadata)

class FieldPolylineWKB(name: String,
                       nullValueAllowed: Boolean,
                       xOrig: Double,
                       yOrig: Double,
                       xyScale: Double,
                       xyTolerance: Double,
                       metadata: Metadata
                      ) extends FieldPolyline(name, ShapeWKB("polyline_wkb", xyTolerance), nullValueAllowed, xOrig, yOrig, xyScale, xyTolerance, metadata)