package com.esri.gdb

import java.nio.ByteBuffer

import com.esri.core.geometry.Polygon
import com.esri.udt.ShapeEsri
import org.apache.spark.sql.types.{DataType, Metadata}

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
      /*
            val polygons = numCoordSeq.map(numCoord => {
              val coordinates = getCoordinates(blob, numCoord)
              geomFact.createLinearRing(coordinates)
            })
            val shell = polygons.head
            val holes = polygons.tail.toArray
            geomFact.createPolygon(shell, holes)
      */
      // TODO - fix shells and holes based on https://github.com/rouault/dump_gdbtable/wiki/FGDB-Spec
      numCoordSeq.foreach(numCoord => addPath(blob, numCoord, polygon))
    }
    else {
      // createPolygon(getCoordinates(blob, numPoints))
      addPath(blob, numPoints, polygon)
    }
    polygon
  }
}

class FieldPolygonEsri(name: String,
                       nullValueAllowed: Boolean,
                       xOrig: Double,
                       yOrig: Double,
                       xyScale: Double,
                       metadata: Metadata
                      ) extends FieldPolygon(name, ShapeEsri("polygon"), nullValueAllowed, xOrig, yOrig, xyScale, metadata)
