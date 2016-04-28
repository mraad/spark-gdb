package com.esri.udt

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.types._

/**
  */
class PointMUDT extends UserDefinedType[PointMType] {

  override def sqlType: DataType = StructType(Seq(
    StructField("x", DoubleType, false),
    StructField("y", DoubleType, false),
    StructField("m", DoubleType, false)
  ))

  override def serialize(obj: Any): InternalRow = {
    obj match {
      case PointMType(x, y, m) => {
        val row = new GenericMutableRow(3)
        row.setDouble(0, x)
        row.setDouble(1, y)
        row.setDouble(2, m)
        row
      }
    }
  }

  override def deserialize(datum: Any): PointMType = {
    datum match {
      case row: InternalRow => PointMType(row.getDouble(0), row.getDouble(1), row.getDouble(2))
    }
  }

  override def userClass: Class[PointMType] = classOf[PointMType]

  override def pyUDT: String = "com.esri.udt.PointMUDT"

  override def typeName: String = "pointM"

  override def equals(o: Any): Boolean = {
    o match {
      case v: PointMUDT => true
      case _ => false
    }
  }

  // see [SPARK-8647], this achieves the needed constant hash code without constant no.
  override def hashCode(): Int = classOf[PointMUDT].getName.hashCode()

  override def asNullable: PointMUDT = this

}