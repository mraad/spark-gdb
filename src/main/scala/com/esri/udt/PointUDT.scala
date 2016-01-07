package com.esri.udt

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.types._

/**
  */
class PointUDT extends UserDefinedType[PointType] {

  override def sqlType: DataType = StructType(Seq(
    StructField("x", DoubleType, false),
    StructField("y", DoubleType, false)
  ))

  override def serialize(obj: Any): InternalRow = {
    obj match {
      case PointType(x, y) => {
        val row = new GenericMutableRow(2)
        row.setDouble(0, x)
        row.setDouble(1, y)
        row
      }
    }
  }

  override def deserialize(datum: Any): PointType = {
    datum match {
      case row: InternalRow => PointType(row.getDouble(0), row.getDouble(1))
    }
  }

  override def userClass: Class[PointType] = classOf[PointType]

  override def pyUDT: String = "com.esri.udt.PointUDT"

  override def typeName: String = "point"

  override def equals(o: Any): Boolean = {
    o match {
      case v: PointUDT => true
      case _ => false
    }
  }

  // see [SPARK-8647], this achieves the needed constant hash code without constant no.
  override def hashCode(): Int = classOf[PointUDT].getName.hashCode()

  override def asNullable: PointUDT = this

}