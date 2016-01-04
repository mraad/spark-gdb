package com.esri.gdb

import java.nio.ByteBuffer

import com.esri.udt.{ShapeJTS, ShapeWKB, ShapeWKT}
import com.vividsolutions.jts.geom.{GeometryFactory, PrecisionModel}
import org.apache.spark.sql.types.{DataType, Metadata}

object FieldPolygon {
  def apply(name: String,
            nullValueAllowed: Boolean,
            xOrig: Double,
            yOrig: Double,
            xyScale: Double,
            xyTolerance: Double,
            metadata: Metadata,
            serde: String) = {
    serde match {
      case "wkt" => new FieldPolygonWKT(name, nullValueAllowed, xOrig, yOrig, xyScale, xyTolerance, metadata)
      case "wkb" => new FieldPolygonWKB(name, nullValueAllowed, xOrig, yOrig, xyScale, xyTolerance, metadata)
      case _ => new FieldPolygonJTS(name, nullValueAllowed, xOrig, yOrig, xyScale, xyTolerance, metadata)
    }
  }
}

abstract class FieldPolygon(name: String,
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
      val polygons = numCoordSeq.map(numCoord => {
        val coordinates = getCoordinates(blob, numCoord)
        geomFact.createLinearRing(coordinates)
      })
      // TODO - fix shells and holes based on https://github.com/rouault/dump_gdbtable/wiki/FGDB-Spec
      val shell = polygons.head
      val holes = polygons.tail.toArray
      geomFact.createPolygon(shell, holes)
    }
    else {
      geomFact.createPolygon(getCoordinates(blob, numPoints))
    }
  }
}

class FieldPolygonJTS(name: String,
                      nullValueAllowed: Boolean,
                      xOrig: Double,
                      yOrig: Double,
                      xyScale: Double,
                      xyTolerance: Double,
                      metadata: Metadata
                     ) extends FieldPolygon(name, ShapeJTS("polygon_jts"), nullValueAllowed, xOrig, yOrig, xyScale, xyTolerance, metadata)

class FieldPolygonWKT(name: String,
                      nullValueAllowed: Boolean,
                      xOrig: Double,
                      yOrig: Double,
                      xyScale: Double,
                      xyTolerance: Double,
                      metadata: Metadata
                     ) extends FieldPolygon(name, ShapeWKT("polygon_wkt", xyTolerance), nullValueAllowed, xOrig, yOrig, xyScale, xyTolerance, metadata)

class FieldPolygonWKB(name: String,
                      nullValueAllowed: Boolean,
                      xOrig: Double,
                      yOrig: Double,
                      xyScale: Double,
                      xyTolerance: Double,
                      metadata: Metadata
                     ) extends FieldPolygon(name, ShapeWKB("polygon_wkb", xyTolerance), nullValueAllowed, xOrig, yOrig, xyScale, xyTolerance, metadata)
