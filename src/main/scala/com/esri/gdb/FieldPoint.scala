package com.esri.gdb

import java.nio.ByteBuffer

import com.esri.udt.{ShapeJTS, ShapeWKB, ShapeWKT}
import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory, PrecisionModel}
import org.apache.spark.sql.types.{DataType, Metadata}

object FieldPoint {
  def apply(name: String,
            nullValueAllowed: Boolean,
            xOrig: Double,
            yOrig: Double,
            xyScale: Double,
            xyTolerance: Double,
            metadata: Metadata,
            serde: String) = {
    serde match {
      case "wkt" => new FieldPointWKT(name, nullValueAllowed, xOrig, yOrig, xyScale, xyTolerance, metadata)
      case "wkb" => new FieldPointWKB(name, nullValueAllowed, xOrig, yOrig, xyScale, xyTolerance, metadata)
      case _ => new FieldPointJTS(name, nullValueAllowed, xOrig, yOrig, xyScale, xyTolerance, metadata)
    }
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

  @transient val geomFact = new GeometryFactory(new PrecisionModel(1.0 / xyTolerance))

  override def readValue(byteBuffer: ByteBuffer, oid: Int) = {
    val blob = getByteBuffer(byteBuffer)

    val geomType = blob getVarUInt

    val vx = blob getVarUInt
    val vy = blob getVarUInt
    val x = (vx - 1.0) / xyScale + xOrig
    val y = (vy - 1.0) / xyScale + yOrig

    geomFact.createPoint(new Coordinate(x, y))
  }
}


class FieldPointJTS(name: String,
                    nullValueAllowed: Boolean,
                    xOrig: Double,
                    yOrig: Double,
                    xyScale: Double,
                    xyTolerance: Double,
                    metadata: Metadata
                   )
  extends FieldPoint(name, ShapeJTS("point_jts"), nullValueAllowed, xOrig, yOrig, xyScale, xyTolerance, metadata)

class FieldPointWKT(name: String,
                    nullValueAllowed: Boolean,
                    xOrig: Double,
                    yOrig: Double,
                    xyScale: Double,
                    xyTolerance: Double,
                    metadata: Metadata
                   )
  extends FieldPoint(name, ShapeWKT("point_wkt", xyTolerance), nullValueAllowed, xOrig, yOrig, xyScale, xyTolerance, metadata)

class FieldPointWKB(name: String,
                    nullValueAllowed: Boolean,
                    xOrig: Double,
                    yOrig: Double,
                    xyScale: Double,
                    xyTolerance: Double,
                    metadata: Metadata
                   )
  extends FieldPoint(name, ShapeWKB("point_wkb", xyTolerance), nullValueAllowed, xOrig, yOrig, xyScale, xyTolerance, metadata)