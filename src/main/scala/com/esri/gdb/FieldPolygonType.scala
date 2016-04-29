package com.esri.gdb

import com.esri.udt.{PolygonType, PolygonUDT}
import org.apache.spark.sql.types.Metadata

/**
  */
object FieldPolygonType extends Serializable {
  def apply(name: String,
            nullValueAllowed: Boolean,
            xOrig: Double,
            yOrig: Double,
            xyScale: Double,
            metadata: Metadata) = {
    new FieldPolygonType(name, nullValueAllowed, xOrig, yOrig, xyScale, metadata)
  }
}

class FieldPolygonType(name: String,
                       nullValueAllowed: Boolean,
                       xOrig: Double,
                       yOrig: Double,
                       xyScale: Double,
                       metadata: Metadata)
  extends FieldPoly2Type[PolygonType](name, new PolygonUDT(), nullValueAllowed, xOrig, yOrig, xyScale, metadata) {

  override def createPolyType(xmin: Double, ymin: Double, xmax: Double, ymax: Double, xyNum: Array[Int], xyArr: Array[Double]): PolygonType = {
    PolygonType(xmin, ymin, xmax, ymax, xyNum, xyArr)
  }
}
