package com.esri.udt

import org.apache.spark.sql.catalyst.InternalRow

/**
  */
class PolylineMUDT extends PolyUDT[PolylineMType] {

  override def serialize(obj: Any): InternalRow = {
    obj match {
      case PolylineMType(xyNum, xyArr) => serialize(xyNum, xyArr)
    }
  }

  override def deserialize(xyNum: Array[Int], xyArr: Array[Double]) = {
    PolylineMType(xyNum, xyArr)
  }

  override def userClass = classOf[PolylineMType]

  override def pyUDT = "com.esri.udt.PolylineMUDT"

  override def typeName = "polylineM"

  override def equals(o: Any) = {
    o match {
      case v: PolylineMUDT => true
      case _ => false
    }
  }

  // see [SPARK-8647], this achieves the needed constant hash code without constant no.
  override def hashCode(): Int = classOf[PolylineMUDT].getName.hashCode()

  override def asNullable: PolylineMUDT = this

}
