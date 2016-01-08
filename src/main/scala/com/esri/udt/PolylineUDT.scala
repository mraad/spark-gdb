package com.esri.udt

import org.apache.spark.sql.catalyst.InternalRow

/**
  */
class PolylineUDT extends PolyUDT[PolylineType] {

  override def serialize(obj: Any): InternalRow = {
    obj match {
      case PolylineType(xmin, ymin, xmax, ymax, xyNum, xyArr) => {
        serialize(xmin, ymin, xmax, ymax, xyNum, xyArr)
      }
    }
  }

  override def deserialize(xmin: Double, ymin: Double, xmax: Double, ymax: Double, xyNum: Array[Int], xyArr: Array[Double]) = {
    PolylineType(xmin, ymin, xmax, ymax, xyNum, xyArr)
  }

  override def userClass = classOf[PolylineType]

  override def pyUDT = "com.esri.udt.PolylineUDT"

  override def typeName = "polyline"

  override def equals(o: Any) = {
    o match {
      case v: PolylineUDT => true
      case _ => false
    }
  }

  // see [SPARK-8647], this achieves the needed constant hash code without constant no.
  override def hashCode(): Int = classOf[PolylineUDT].getName.hashCode()

  override def asNullable: PolylineUDT = this

}
