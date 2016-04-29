package com.esri.gdb

import com.esri.udt.{PolylineType, PolylineUDT}
import org.apache.spark.sql.types.Metadata

/**
  */
object FieldPolylineType extends Serializable {
  def apply(name: String,
            nullValueAllowed: Boolean,
            xOrig: Double,
            yOrig: Double,
            xyScale: Double,
            metadata: Metadata) = {
    new FieldPolylineType(name, nullValueAllowed, xOrig, yOrig, xyScale, metadata)
  }
}

class FieldPolylineType(name: String,
                        nullValueAllowed: Boolean,
                        xOrig: Double,
                        yOrig: Double,
                        xyScale: Double,
                        metadata: Metadata)
  extends FieldPoly2Type[PolylineType](name, new PolylineUDT(), nullValueAllowed, xOrig, yOrig, xyScale, metadata) {

  override def createPolyType(xmin: Double, ymin: Double, xmax: Double, ymax: Double, xyNum: Array[Int], xyArr: Array[Double]): PolylineType = {
    PolylineType(xmin, ymin, xmax, ymax, xyNum, xyArr)
  }
}
