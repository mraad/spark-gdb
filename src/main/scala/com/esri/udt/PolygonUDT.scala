package com.esri.udt

import org.apache.spark.sql.catalyst.InternalRow

/**
  */
class PolygonUDT extends PolyUDT[PolygonType] {

  override def serialize(obj: Any): InternalRow = {
    obj match {
      case PolygonType(xyNum, xyArr) => {
        serialize(xyNum, xyArr)
      }
    }
  }

  override def deserialize(xyNum: Array[Int], xyArr: Array[Double]) = {
    PolygonType(xyNum, xyArr)
  }

  override def userClass = classOf[PolygonType]

  override def pyUDT = "com.esri.udt.PolygonUDT"

  override def typeName = "polygon"

  override def equals(o: Any) = {
    o match {
      case v: PolygonUDT => true
      case _ => false
    }
  }

  // see [SPARK-8647], this achieves the needed constant hash code without constant no.
  override def hashCode(): Int = classOf[PolygonUDT].getName.hashCode()

  override def asNullable: PolygonUDT = this

}
